import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { HStack } from "components/common/utilities/HStack";
import EditGenAiTaskBasicFields from "../fields/EditGenAiTaskBasicFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { useServices } from "components/hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";

export function EditGenAiTaskStepBasic() {
    const dispatch = useAppDispatch();

    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const aiConnectionStrings = useAppSelector(editGenAiTaskSelectors.aiConnectionStrings);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const handleNext = async () => {
        const isValid = await trigger(["name", "state", "responsibleNode", "connectionStringName"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.currentStepSet("context"));
        }
    };

    const asyncHandleTest = useAsyncCallback(async () => {
        const connectionString = aiConnectionStrings[formValues.connectionStringName];

        return tasksService.testAiConnectionString(
            databaseName,
            getConnectorType(connectionString),
            mapAiConnectionStringToSettingsDto(connectionString)
        );
    });

    return (
        <div>
            <AboutViewHeading title="Configure GenAI task settings" marginBottom={4} icon="ai-etl" />
            <EditGenAiTaskBasicFields />
            <div className="mt-2">
                <ConnectionTestResult testResult={asyncHandleTest.result} />
            </div>

            <HStack gap={2} className="justify-content-end mt-3">
                <ButtonWithSpinner
                    variant="info"
                    className="rounded-pill"
                    onClick={asyncHandleTest.execute}
                    isSpinning={asyncHandleTest.loading}
                    icon="test"
                >
                    Test connection
                </ButtonWithSpinner>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </HStack>
        </div>
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

    return "None";
};

type Settings =
    | Raven.Client.Documents.Operations.AI.OpenAiSettings
    | Raven.Client.Documents.Operations.AI.AzureOpenAiSettings
    | Raven.Client.Documents.Operations.AI.OllamaSettings
    | Raven.Client.Documents.Operations.AI.EmbeddedSettings
    | Raven.Client.Documents.Operations.AI.GoogleSettings
    | Raven.Client.Documents.Operations.AI.HuggingFaceSettings
    | Raven.Client.Documents.Operations.AI.MistralAiSettings;

export function mapAiConnectionStringToSettingsDto(
    connection: Raven.Client.Documents.Operations.AI.AiConnectionString
): Settings {
    if (connection.AzureOpenAiSettings) {
        return connection.AzureOpenAiSettings;
    }
    if (connection.GoogleSettings) {
        return connection.GoogleSettings;
    }
    if (connection.HuggingFaceSettings) {
        return connection.HuggingFaceSettings;
    }
    if (connection.OllamaSettings) {
        return connection.OllamaSettings;
    }
    if (connection.EmbeddedSettings) {
        return connection.EmbeddedSettings;
    }
    if (connection.OpenAiSettings) {
        return connection.OpenAiSettings;
    }
    if (connection.MistralAiSettings) {
        return connection.MistralAiSettings;
    }

    return null;
}
