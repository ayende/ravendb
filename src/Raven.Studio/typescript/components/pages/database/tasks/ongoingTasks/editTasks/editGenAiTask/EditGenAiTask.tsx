import "./EditGenAiTask.scss";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { yupResolver } from "@hookform/resolvers/yup";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";
import { tryHandleSubmit } from "components/utils/common";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "./store/editGenAiTaskSlice";
import { useEffect } from "react";
import { useEditGenAiTaskSteps } from "./hooks/useEditGenAiTaskSteps";
import { EditGenAiTaskFormData, editGenAiTaskSchema } from "./utils/editGenAiTaskValidation";
import { editGenAiTaskUtils } from "./utils/editGenAiTaskUtils";
import EditGenAiTaskTestResults from "./partials/EditGenAiTaskTestResults";
import EditGenAiTaskSteps from "./partials/EditGenAiTaskSteps";

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
        mode: "all",
        resolver: yupResolver(editGenAiTaskSchema),
        defaultValues: async () => {
            if (taskId) {
                const dto = await tasksService.getGenAiTaskInfo(databaseName, taskId);
                return editGenAiTaskUtils.getDefaultValues(dto);
            }

            return editGenAiTaskUtils.getDefaultValues(null);
        },
    });

    const { handleSubmit, reset } = form;

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

    const steps = useEditGenAiTaskSteps();
    const currentStep = steps.find((x) => x.isCurrent);

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(handleSave)} className="edit-gen-ai-task">
                <div className="main-container">{currentStep.component}</div>
                <div className="footer">{currentStep.footer}</div>
                <div className="sidebar">
                    {isTestOpen ? <EditGenAiTaskTestResults /> : <EditGenAiTaskSteps steps={steps} />}
                </div>
            </form>
        </FormProvider>
    );
}
