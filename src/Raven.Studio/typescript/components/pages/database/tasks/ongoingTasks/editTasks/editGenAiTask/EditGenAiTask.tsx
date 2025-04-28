import "./EditGenAiTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import { HStack } from "components/common/utilities/HStack";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";
import { tryHandleSubmit } from "components/utils/common";
import classNames from "classnames";
import { Switch } from "components/common/Checkbox";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "./store/editGenAiTaskSlice";
import { useEffect } from "react";
import { useEditGenAiTaskSteps } from "./hooks/useEditGenAiTaskSteps";
import { NumberedList } from "components/common/NumberedList";
import ListStepItem from "components/common/ListStepItem";
import { EditGenAiTaskFormData, editGenAiTaskSchema } from "./utils/editGenAiTaskValidation";
import { editGenAiTaskUtils } from "./utils/editGenAiTaskUtils";
import EditGenAiTaskAdvancedMode from "./partials/EditGenAiTaskAdvancedMode";
import ProgressBar from "react-bootstrap/ProgressBar";

interface QueryParams {
    taskId: string;
    sourceView: EditAiTaskSourceView;
}

export default function EditGenAiTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();

    const { tasksService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isNewTask = useAppSelector(editGenAiTaskSelectors.isNewTask);
    const isAdvancedMode = useAppSelector(editGenAiTaskSelectors.isAdvancedMode);

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId) : null;

    // Get query params
    useEffect(() => {
        if (queryParams) {
            dispatch(editGenAiTaskActions.taskIdSet(taskId));
            dispatch(editGenAiTaskActions.sourceViewSet(queryParams.sourceView));
        }

        return () => {
            dispatch(editGenAiTaskActions.reset());
        };
    }, []);

    const form = useForm<EditGenAiTaskFormData>({
        resolver: yupResolver(editGenAiTaskSchema),
        defaultValues: async () => {
            if (taskId) {
                const dto = await tasksService.getGenAiTaskInfo(databaseName, taskId);
                return editGenAiTaskUtils.getDefaultValues(dto);
            }

            return editGenAiTaskUtils.getDefaultValues(null);
        },
    });

    const { handleSubmit, formState, reset } = form;

    const { appUrl } = useAppUrls();

    const handleSave: SubmitHandler<EditGenAiTaskFormData> = (data) => {
        return tryHandleSubmit(async () => {
            const scriptsToReset = data.isResetScript ? [data.scriptToReset] : undefined;
            await tasksService.saveGenAiTask(databaseName, editGenAiTaskUtils.mapToDto(data, taskId), scriptsToReset);
            reset(data);
            goBack();
        });
    };

    const goBack = () => {
        if (queryParams?.sourceView === "AiTasks") {
            router.navigate(appUrl.forAiTasks(databaseName));
        } else {
            router.navigate(appUrl.forOngoingTasks(databaseName));
        }
    };

    console.log("kalczur errors", formState.errors);

    const steps = useEditGenAiTaskSteps();
    const currentStep = steps.find((x) => x.isCurrent);
    const currentStepIdx = steps.findIndex((x) => x.isCurrent);

    return (
        <div className="parent">
            <div className="div1">
                <HStack className="align-items-center mb-4">
                    <AboutViewHeading title={isNewTask ? "New GenAI" : "Edit GenAI"} marginBottom={0} icon="ai-etl" />
                    <Switch
                        color="primary"
                        selected={isAdvancedMode}
                        toggleSelection={() => dispatch(editGenAiTaskActions.isAdvancedModeSet(!isAdvancedMode))}
                        className="ms-2"
                    >
                        Advanced mode
                    </Switch>
                </HStack>

                <FormProvider {...form}>
                    <form onSubmit={handleSubmit(handleSave)}>
                        {isAdvancedMode ? <EditGenAiTaskAdvancedMode /> : currentStep.component}
                    </form>
                </FormProvider>
            </div>
            <div className="div2">
                {!isAdvancedMode && (
                    <div className="flex-grow">
                        <div className="mb-3">
                            <span>
                                {currentStepIdx}/{steps.length} steps completed
                            </span>
                            <ProgressBar
                                now={currentStepIdx}
                                max={steps.length}
                                variant="primary"
                                style={{ height: 7 }}
                                className="w-50 mt-1"
                            />
                        </div>
                        <NumberedList>
                            {steps.map((step, idx) => (
                                <ListStepItem
                                    key={step.title}
                                    isCurrent={step.isCurrent}
                                    isChecked={idx < currentStepIdx}
                                    isInactive={idx > currentStepIdx}
                                    className={classNames("cursor-pointer", {
                                        "cursor-not-allowed": idx > currentStepIdx,
                                    })}
                                >
                                    <h5 className="mb-0" style={{ paddingTop: 4 }}>
                                        {step.title}
                                    </h5>
                                </ListStepItem>
                            ))}
                        </NumberedList>
                    </div>
                )}
            </div>
        </div>
    );
}
