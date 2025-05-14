import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormInput, FormSwitch, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "components/hooks/useBoolean";
import EditConnectionStrings from "components/pages/database/settings/connectionStrings/EditConnectionStrings";
import { useAppDispatch, useAppSelector } from "components/store";
import { sortBy } from "lodash";
import { useAsync } from "react-async-hook";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskNodeField from "./EditGenAiTaskNodeField";
import InputGroup from "react-bootstrap/InputGroup";
import { useServices } from "components/hooks/useServices";
import { useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditGenAiTaskBasicFields() {
    const dispatch = useAppDispatch();

    const isNewTask = useAppSelector(editGenAiTaskSelectors.isNewTask);
    const isEncrypted = useAppSelector(databaseSelectors.activeDatabase)?.isEncrypted;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { value: isNewConnectionStringOpen, toggle: toggleIsNewConnectionStringOpen } = useBoolean(false);

    const { tasksService } = useServices();

    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const asyncGetConnectionStringsOptions = useAsync(async () => {
        const result = await tasksService.getConnectionStrings(databaseName);

        dispatch(editGenAiTaskActions.aiConnectionStringsSet(result.AiConnectionStrings));

        const connectionStrings = Object.values(result.AiConnectionStrings).map((x) => x.Name);

        return sortBy(connectionStrings, (x) => x.toUpperCase()).map(
            (x) => ({ value: x, label: x }) satisfies SelectOption
        );
    }, []);

    const handleConnectionStringSave = async (connectionName: string) => {
        await asyncGetConnectionStringsOptions.execute();
        setValue("connectionStringName", connectionName, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
        });
        toggleIsNewConnectionStringOpen();
    };

    return (
        <>
            <FormGroup>
                <FormLabel>Task Name</FormLabel>
                <FormInput type="text" control={control} name="name" />
            </FormGroup>
            <FormGroup>
                <FormLabel>Task State</FormLabel>
                <FormSelect control={control} name="state" options={stateOptions} />
            </FormGroup>
            {isEncrypted && (
                <div className="vstack gap-2">
                    <RichAlert variant="info">
                        Database <strong>{databaseName}</strong> is encrypted
                    </RichAlert>
                    <FormGroup>
                        <FormSwitch control={control} name="isAllowEtlOnNonEncryptedChannel">
                            Allow task on a non-encrypted communication channel
                        </FormSwitch>
                    </FormGroup>
                </div>
            )}
            <EditGenAiTaskNodeField />
            <FormGroup>
                <FormLabel>Connection String</FormLabel>
                <InputGroup>
                    <FormSelect
                        control={control}
                        name="connectionStringName"
                        options={asyncGetConnectionStringsOptions.result ?? []}
                        isLoading={asyncGetConnectionStringsOptions.loading}
                    />
                    <InputGroup.Text>
                        <ButtonWithSpinner
                            variant="link"
                            className="text-reset px-0"
                            icon="plus"
                            isSpinning={asyncGetConnectionStringsOptions.loading}
                            onClick={toggleIsNewConnectionStringOpen}
                        >
                            Create a new AI connection string
                        </ButtonWithSpinner>
                    </InputGroup.Text>
                    {isNewConnectionStringOpen && (
                        <EditConnectionStrings
                            initialConnection={{ type: "Ai" }}
                            afterSave={handleConnectionStringSave}
                            afterClose={toggleIsNewConnectionStringOpen}
                        />
                    )}
                </InputGroup>
            </FormGroup>
            {!isNewTask && (
                <FormGroup>
                    <FormSwitch control={control} name="isResetScript">
                        Regenerate all documents
                    </FormSwitch>
                </FormGroup>
            )}
        </>
    );
}

const stateOptions: SelectOption<OngoingTaskState>[] = (["Enabled", "Disabled"] satisfies OngoingTaskState[]).map(
    (x) => ({
        label: x,
        value: x,
    })
);
