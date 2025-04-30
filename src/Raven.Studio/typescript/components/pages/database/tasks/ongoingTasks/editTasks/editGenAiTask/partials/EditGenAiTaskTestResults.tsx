import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import AceEditor from "components/common/AceEditor";

export default function EditGenAiTaskTestResults() {
    const testStage = useAppSelector(editGenAiTaskSelectors.testStage);

    const contextTestResults = useAppSelector(editGenAiTaskSelectors.contextTestResults);
    const modelOutputTestResults = useAppSelector(editGenAiTaskSelectors.modelOutputTestResults);
    const updateScriptTestResult = useAppSelector(editGenAiTaskSelectors.updateScriptTestResult);

    return (
        <div>
            {testStage === "CreateContextObjects" && contextTestResults.length > 0 && (
                <div>
                    {contextTestResults.map((x, idx) => (
                        <AceEditor key={idx} mode="json" value={x} readOnly />
                    ))}
                </div>
            )}
            {testStage === "SendToModel" && modelOutputTestResults.length > 0 && (
                <div>
                    {modelOutputTestResults.map((x, idx) => (
                        <AceEditor key={idx} mode="json" value={x} readOnly />
                    ))}
                </div>
            )}
            {testStage === "ApplyUpdateScript" && updateScriptTestResult && (
                <AceEditor mode="json" value={updateScriptTestResult} readOnly />
            )}
        </div>
    );
}
