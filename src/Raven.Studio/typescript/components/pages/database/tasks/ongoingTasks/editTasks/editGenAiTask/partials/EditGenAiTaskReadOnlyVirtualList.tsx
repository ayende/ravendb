import { useVirtualizer } from "@tanstack/react-virtual";
import AceEditor from "components/common/AceEditor";
import { useAppDispatch, useAppSelector } from "components/store";
import { useRef } from "react";
import Badge from "react-bootstrap/Badge";
import classNames from "classnames";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";

interface EditGenAiTaskReadOnlyVirtualListProps {
    data: string[];
}

export default function EditGenAiTaskReadOnlyVirtualList({ data }: EditGenAiTaskReadOnlyVirtualListProps) {
    const dispatch = useAppDispatch();

    const hoverIndex = useAppSelector(editGenAiTaskSelectors.hoverIndex);

    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: data.length,
        estimateSize: () => 200,
        getScrollElement: () => listRef.current,
        overscan: 5,
    });

    return (
        <div className="flex-grow-1 overflow-auto" ref={listRef}>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualizer.getVirtualItems().map((virtualRow) => {
                    const entry = data[virtualRow.index];

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
                                <AceEditor key={virtualRow.key} mode="json" value={entry} readOnly={true} />
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
