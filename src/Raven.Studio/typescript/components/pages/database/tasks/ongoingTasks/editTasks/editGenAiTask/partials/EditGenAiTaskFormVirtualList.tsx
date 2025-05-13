import { useVirtualizer } from "@tanstack/react-virtual";
import { EmptySet } from "components/common/EmptySet";
import { FormAceEditor } from "components/common/Form";
import { useRef } from "react";
import { FieldArrayWithId, FieldPath, useFormContext } from "react-hook-form";
import { EditGenAiTaskFormData } from "../utils/editGenAiTaskValidation";
import Badge from "react-bootstrap/Badge";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import classNames from "classnames";

interface EditGenAiTaskFormVirtualListProps {
    fields: FieldArrayWithId<EditGenAiTaskFormData>[];
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
    isReadOnly: boolean;
}

export default function EditGenAiTaskFormVirtualList({ fields, name, isReadOnly }: EditGenAiTaskFormVirtualListProps) {
    const dispatch = useAppDispatch();

    const hoverIndex = useAppSelector(editGenAiTaskSelectors.hoverIndex);

    const { control } = useFormContext<EditGenAiTaskFormData>();

    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: fields.length,
        estimateSize: () => 200,
        getScrollElement: () => listRef.current,
        overscan: 5,
    });

    if (fields.length === 0) {
        return <EmptySet />;
    }

    return (
        <div className="flex-grow-1 overflow-auto" ref={listRef}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const field = fields[virtualRow.index];
                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            ref={virtualizer.measureElement}
                            className={classNames("py-1", {
                                "ace-hover": hoverIndex === virtualRow.index,
                            })}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                transition: "unset",
                            }}
                            onMouseEnter={() => dispatch(editGenAiTaskActions.hoverIndexSet(virtualRow.index))}
                            onMouseLeave={() => dispatch(editGenAiTaskActions.hoverIndexSet(null))}
                        >
                            <div style={{ position: "relative" }}>
                                <FormAceEditor
                                    key={field.id}
                                    control={control}
                                    name={`${name}.${virtualRow.index}.value`}
                                    mode="json"
                                    readOnly={isReadOnly}
                                />
                                <Badge bg="secondary" style={{ position: "absolute", bottom: 10, right: 10 }}>
                                    {virtualRow.index + 1}
                                </Badge>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
