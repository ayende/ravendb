import "./EditGenAiTask.scss";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";
import { tryHandleSubmit } from "components/utils/common";
import classNames from "classnames";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "./store/editGenAiTaskSlice";
import { useEffect } from "react";
import { useEditGenAiTaskSteps } from "./hooks/useEditGenAiTaskSteps";
import { NumberedList } from "components/common/NumberedList";
import ListStepItem from "components/common/ListStepItem";
import { EditGenAiTaskFormData, editGenAiTaskSchema } from "./utils/editGenAiTaskValidation";
import { editGenAiTaskUtils } from "./utils/editGenAiTaskUtils";
import ProgressBar from "react-bootstrap/ProgressBar";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import EditGenAiTaskTestResults from "./partials/EditGenAiTaskTestResults";

interface QueryParams {
    taskId: string;
    sourceView: EditAiTaskSourceView;
}

export default function EditGenAiTask({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();

    const { tasksService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isTestOpen = useAppSelector(editGenAiTaskSelectors.isTestOpen);

    const taskId = queryParams?.taskId ? parseInt(queryParams.taskId) : null;

    // Set query params to store
    useEffect(() => {
        if (taskId) {
            dispatch(editGenAiTaskActions.taskIdSet(taskId));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }

        if (queryParams) {
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

    // TODO move steps to component

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(handleSave)} className="h-100">
                <Row className="h-100 m-0">
                    <Col md={isTestOpen ? 6 : 8} className="p-4">
                        {currentStep.component}
                    </Col>
                    <Col md={isTestOpen ? 6 : 4} className="panel-bg-1 p-4">
                        {!isTestOpen && (
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
                                            onClick={() => {
                                                if (idx > currentStepIdx) {
                                                    return;
                                                }

                                                dispatch(editGenAiTaskActions.currentStepSet(step.id));
                                            }}
                                        >
                                            <h5 className="mb-0" style={{ paddingTop: 4 }}>
                                                {step.title}
                                            </h5>
                                        </ListStepItem>
                                    ))}
                                </NumberedList>
                            </div>
                        )}
                        {isTestOpen && <EditGenAiTaskTestResults />}
                    </Col>
                </Row>
            </form>
        </FormProvider>
    );
}
