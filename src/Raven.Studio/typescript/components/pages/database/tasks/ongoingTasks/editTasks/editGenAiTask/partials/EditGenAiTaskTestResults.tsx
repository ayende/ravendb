import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import AceEditor from "components/common/AceEditor";
import aceDiff from "common/helpers/text/aceDiff";
import ReactAce from "react-ace/lib/ace";
import { useRef, useEffect } from "react";
import { HStack } from "components/common/utilities/HStack";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { VStack } from "components/common/utilities/VStack";

export default function EditGenAiTaskTestResults() {
    const dispatch = useAppDispatch();

    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const contextTestResults = useAppSelector(editGenAiTaskSelectors.contextTestResults);
    const modelOutputTestResults = useAppSelector(editGenAiTaskSelectors.modelOutputTestResults);
    const updateScriptTestResult = useAppSelector(editGenAiTaskSelectors.updateScriptTestResult);

    return (
        <div>
            {currentStep === "context" && contextTestResults.length > 0 && (
                <div>
                    <HStack className="mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            Task context test results
                        </h3>
                        <HStack gap={2}>
                            <Button variant="primary" className="rounded-pill">
                                <Icon icon="reset" />
                                Re-run test
                            </Button>
                            <Button
                                variant="secondary"
                                className="rounded-pill"
                                onClick={() => dispatch(editGenAiTaskActions.testStageSet(null))}
                            >
                                <Icon icon="cancel" />
                                Close
                            </Button>
                        </HStack>
                    </HStack>
                    <VStack gap={2}>
                        {contextTestResults.map((x, idx) => (
                            <AceEditor key={idx} mode="json" value={x} readOnly />
                        ))}
                    </VStack>
                </div>
            )}
            {currentStep === "modelInput" && modelOutputTestResults.length > 0 && (
                <div>
                    <HStack className="mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            Model input test results
                        </h3>
                        <HStack gap={2}>
                            <Button variant="primary" className="rounded-pill">
                                <Icon icon="reset" />
                                Re-run test
                            </Button>
                            <Button
                                variant="secondary"
                                className="rounded-pill"
                                onClick={() => dispatch(editGenAiTaskActions.testStageSet(null))}
                            >
                                <Icon icon="cancel" />
                                Close
                            </Button>
                        </HStack>
                    </HStack>
                    <VStack gap={2}>
                        {modelOutputTestResults.map((x, idx) => (
                            <AceEditor key={idx} mode="json" value={x} readOnly />
                        ))}
                    </VStack>
                </div>
            )}
            {currentStep === "updateScript" && updateScriptTestResult && <UpdateScriptResult />}
        </div>
    );
}

function UpdateScriptResult() {
    const dispatch = useAppDispatch();

    const oldDoc = JSON.stringify(useAppSelector(editGenAiTaskSelectors.globalTestResult).InputDocument, null, 4);
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

    // TODO add ace editor title

    return (
        <div>
            <HStack className="mb-3 justify-content-between">
                <h3 className="mb-0">
                    <Icon icon="test" />
                    Update script test results
                </h3>
                <HStack gap={2}>
                    <Button variant="primary" className="rounded-pill">
                        <Icon icon="reset" />
                        Re-run test
                    </Button>
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        onClick={() => dispatch(editGenAiTaskActions.testStageSet(null))}
                    >
                        <Icon icon="cancel" />
                        Close
                    </Button>
                </HStack>
            </HStack>
            <VStack gap={2}>
                <div>
                    <AceEditor aceRef={oldDocRef} value={oldDoc} mode="json" height="100px" />
                </div>
                <div>
                    <AceEditor aceRef={newDocRef} value={newDoc} mode="json" height="100px" />
                </div>
            </VStack>
        </div>
    );
}
