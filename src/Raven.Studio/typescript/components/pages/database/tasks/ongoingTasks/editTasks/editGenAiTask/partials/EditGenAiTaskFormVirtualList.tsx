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
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";

interface EditGenAiTaskFormVirtualListProps {
    fields: FieldArrayWithId<EditGenAiTaskFormData>[];
    name: Extract<FieldPath<EditGenAiTaskFormData>, "playgroundContexts" | "playgroundModelOutputs">;
    isReadOnly: boolean;
    handleRemove: (index: number) => void;
}

export default function EditGenAiTaskFormVirtualList({
    fields,
    name,
    isReadOnly,
    handleRemove,
}: EditGenAiTaskFormVirtualListProps) {
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
        return <EmptySet>Empty list</EmptySet>;
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
                                "ace-hover": hoverIndex === field.idx,
                            })}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                transform: `translateY(${virtualRow.start}px)`,
                                transition: "unset",
                            }}
                            onMouseEnter={() => dispatch(editGenAiTaskActions.hoverIndexSet(field.idx))}
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
                                {!isReadOnly && (
                                    <Button
                                        variant="danger"
                                        style={{ position: "absolute", top: 10, right: 10 }}
                                        onClick={() => handleRemove(virtualRow.index)}
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                )}
                                <Badge bg="secondary" style={{ position: "absolute", bottom: 10, right: 10 }}>
                                    {field.idx != null ? field.idx + 1 : "?"}
                                </Badge>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
