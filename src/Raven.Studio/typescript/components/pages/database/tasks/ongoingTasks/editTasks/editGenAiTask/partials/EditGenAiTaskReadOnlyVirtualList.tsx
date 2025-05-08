import { useVirtualizer } from "@tanstack/react-virtual";
import AceEditor from "components/common/AceEditor";
import SizeGetter from "components/common/SizeGetter";
import { useRef } from "react";

interface EditGenAiTaskReadOnlyVirtualListProps {
    data: string[];
}

export default function EditGenAiTaskReadOnlyVirtualList({ data }: EditGenAiTaskReadOnlyVirtualListProps) {
    const listRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: data.length,
        estimateSize: () => 200,
        getScrollElement: () => listRef.current,
        overscan: 5,
    });

    return (
        <div className="flex-grow-1">
            <SizeGetter
                isHeighRequired
                render={(size) => (
                    <div className="overflow-auto" style={{ height: size.height }} ref={listRef}>
                        <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                            {virtualizer.getVirtualItems().map((virtualRow) => {
                                const entry = data[virtualRow.index];

                                return (
                                    <div
                                        key={virtualRow.key}
                                        data-index={virtualRow.index}
                                        ref={virtualizer.measureElement}
                                        className="hover-filter py-1"
                                        style={{
                                            position: "absolute",
                                            top: 0,
                                            left: 0,
                                            width: "100%",
                                            transform: `translateY(${virtualRow.start}px)`,
                                            transition: "unset",
                                        }}
                                    >
                                        <AceEditor key={virtualRow.key} mode="json" value={entry} readOnly={true} />
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                )}
            />
        </div>
    );
}
