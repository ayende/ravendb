import React, { ReactNode, PropsWithChildren } from "react";
import { PopoverBody, UncontrolledPopover } from "reactstrap";
import useUniqueId from "components/hooks/useUniqueId";
import { Placement } from "@popperjs/core";
import { ClassNameProps } from "components/models/common";
import classNames from "classnames";

interface Condition {
    isActive: boolean;
    message?: ReactNode | ReactNode[];
}

interface ConditionalPopoverProps extends Required<PropsWithChildren>, ClassNameProps {
    conditions: Condition | Condition[];
    popoverPlacement?: Placement;
}

export function ConditionalPopover(props: ConditionalPopoverProps) {
    const { children, popoverPlacement, className } = props;

    const containerId = useUniqueId("conditional-popover-");

    const conditions = Array.isArray(props.conditions) ? props.conditions : [props.conditions];
    const message = conditions.find((x) => x.isActive)?.message;

    return (
        <>
            <div id={containerId} className={classNames("d-flex w-fit-content", className)}>
                {children}
            </div>

            {message && (
                <UncontrolledPopover target={containerId} trigger="hover" placement={popoverPlacement} className="bs5">
                    <PopoverBody>{message}</PopoverBody>
                </UncontrolledPopover>
            )}
        </>
    );
}
