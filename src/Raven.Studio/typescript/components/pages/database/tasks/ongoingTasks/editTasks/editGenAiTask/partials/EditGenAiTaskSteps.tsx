import classNames from "classnames";
import ListStepItem from "components/common/ListStepItem";
import { NumberedList } from "components/common/NumberedList";
import ProgressBar from "react-bootstrap/ProgressBar";
import { EditGenAiTaskStep } from "../hooks/useEditGenAiTaskSteps";
import { editGenAiTaskActions } from "../store/editGenAiTaskSlice";
import { useAppDispatch } from "components/store";

interface EditGenAiTaskStepsProps {
    steps: EditGenAiTaskStep[];
}

export default function EditGenAiTaskSteps({ steps }: EditGenAiTaskStepsProps) {
    const dispatch = useAppDispatch();

    const currentStepIdx = steps.findIndex((x) => x.isCurrent);

    return (
        <>
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
        </>
    );
}
