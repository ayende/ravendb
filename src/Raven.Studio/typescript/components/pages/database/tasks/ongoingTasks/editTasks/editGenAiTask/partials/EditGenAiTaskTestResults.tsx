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
import { useEditGenAiTaskTests } from "../hooks/useEditGenAiTaskTests";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

export default function EditGenAiTaskTestResults() {
    const dispatch = useAppDispatch();

    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);
    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);
    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    console.log("kalczur c", { currentStep, modelInputTest });

    const { handleContextTest } = useEditGenAiTaskTests();

    return (
        <div>
            {currentStep === "context" && contextTest.data?.length > 0 && (
                <div>
                    <HStack className="mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            Task context test results
                        </h3>
                        <HStack gap={2}>
                            <ButtonWithSpinner
                                variant="primary"
                                className="rounded-pill"
                                onClick={handleContextTest}
                                icon="reset"
                                isSpinning={contextTest.status === "loading"}
                            >
                                Re-run test
                            </ButtonWithSpinner>
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
                        {contextTest.data.map((x, idx) => (
                            <AceEditor key={idx} mode="json" value={x} readOnly />
                        ))}
                    </VStack>
                </div>
            )}
            {currentStep === "modelInput" && modelInputTest.data?.length > 0 && (
                <div>
                    <HStack className="mb-3 justify-content-between">
                        <h3 className="mb-0">
                            <Icon icon="test" />
                            Model input test results
                        </h3>
                        <HStack gap={2}>
                            <ButtonWithSpinner
                                variant="primary"
                                className="rounded-pill"
                                icon="reset"
                                isSpinning={modelInputTest.status === "loading"}
                            >
                                Re-run test
                            </ButtonWithSpinner>
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
                        {modelInputTest.data.map((x, idx) => (
                            <AceEditor key={idx} mode="json" value={x} readOnly />
                        ))}
                    </VStack>
                </div>
            )}
            {currentStep === "updateScript" && updateScriptTest.data != null && <UpdateScriptResult />}
        </div>
    );
}

function UpdateScriptResult() {
    const dispatch = useAppDispatch();

    const oldDoc = JSON.stringify(useAppSelector(editGenAiTaskSelectors.globalTestResult).InputDocument, null, 4);
    const newDoc = useAppSelector(editGenAiTaskSelectors.updateScriptTest).data ?? "";

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
