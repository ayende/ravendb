import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { HStack } from "components/common/utilities/HStack";
import EditGenAiTaskBasicFields from "../fields/EditGenAiTaskBasicFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";

export function EditGenAiTaskStepBasic() {
    const connectionStringTest = useAppSelector(editGenAiTaskSelectors.connectionStringTest);

    return (
        <div>
            <AboutViewHeading title="Configure GenAI task settings" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskBasicFields />
            <div className="mt-2">
                <ConnectionTestResult testResult={connectionStringTest.data} />
            </div>
        </div>
    );
}

export function EditGenAiTaskStepBasicFooter() {
    const dispatch = useAppDispatch();

    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const connectionStringTest = useAppSelector(editGenAiTaskSelectors.connectionStringTest);
    const aiConnectionStrings = useAppSelector(editGenAiTaskSelectors.aiConnectionStrings);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const handleTest = async () => {
        const connectionString = aiConnectionStrings[formValues.connectionStringName];

        dispatch(
            editGenAiTaskActions.testConnectionString({
                databaseName,
                connectorType: getConnectorType(connectionString),
                settings: mapAiConnectionStringToSettingsDto(connectionString),
            })
        );
    };

    const handleNext = async () => {
        const isValid = await trigger(["name", "state", "responsibleNode", "connectionStringName"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("context"));
        }
    };

    return (
        <HStack gap={2} className="justify-content-end">
            <ButtonWithSpinner
                variant="info"
                className="rounded-pill"
                onClick={handleTest}
                isSpinning={connectionStringTest.status === "loading"}
                icon="test"
            >
                Test connection
            </ButtonWithSpinner>

            <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                Next <Icon icon="arrow-right" margin="ms-1" />
            </Button>
        </HStack>
    );
}

const getConnectorType = (
    connection: Raven.Client.Documents.Operations.AI.AiConnectionString
): Raven.Client.Documents.Operations.AI.AiConnectorType => {
    if (connection.AzureOpenAiSettings) {
        return "AzureOpenAi";
    }
    if (connection.GoogleSettings) {
        return "Google";
    }
    if (connection.HuggingFaceSettings) {
        return "HuggingFace";
    }
    if (connection.OllamaSettings) {
        return "Ollama";
    }
    if (connection.EmbeddedSettings) {
        return "Embedded";
    }
    if (connection.OpenAiSettings) {
        return "OpenAi";
    }
    if (connection.MistralAiSettings) {
        return "MistralAi";
    }

    throw new Error("No connector type found. Please check the connection string.");
};

export function mapAiConnectionStringToSettingsDto(
    connection: Raven.Client.Documents.Operations.AI.AiConnectionString
): AiConnectionStringsSettings {
    const settings = [
        connection.AzureOpenAiSettings,
        connection.GoogleSettings,
        connection.HuggingFaceSettings,
        connection.OllamaSettings,
        connection.EmbeddedSettings,
        connection.OpenAiSettings,
        connection.MistralAiSettings,
    ].find(Boolean);

    if (!settings) {
        throw new Error("No settings found. Please check the connection string.");
    }

    return settings;
}
