import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import AceEditor from "components/common/AceEditor";
import aceDiff from "common/helpers/text/aceDiff";
import ReactAce from "react-ace/lib/ace";
import { useRef, useEffect } from "react";

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
            {testStage === "ApplyUpdateScript" && updateScriptTestResult && <UpdateScriptResult />}
        </div>
    );
}

function UpdateScriptResult() {
    const oldDoc = ""; // TODO
    const newDoc = useAppSelector(editGenAiTaskSelectors.updateScriptTestResult);

    const oldDocRef = useRef<ReactAce>(null);
    const newDocRef = useRef<ReactAce>(null);

    useEffect(() => {
        if (!oldDocRef.current || !newDocRef.current) {
            return;
        }

        // We have different ace versions, lets just use 'any' here instead of editing aceDiff class
        const aceDiffC = new aceDiff(oldDocRef.current.editor as any, newDocRef.current.editor as any, false);
        aceDiffC.refresh(false);

        return () => {
            aceDiffC.destroy();
        };
    }, [oldDoc, newDoc]);

    return (
        <div className="vstack gap-2">
            <div>
                <AceEditor aceRef={oldDocRef} value={oldDoc} mode="json" height="100px" />
            </div>
            <div>
                <AceEditor aceRef={newDocRef} value={newDoc} mode="json" height="100px" />
            </div>
        </div>
    );
}
