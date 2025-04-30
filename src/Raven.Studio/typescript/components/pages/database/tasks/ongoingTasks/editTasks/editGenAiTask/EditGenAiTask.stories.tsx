import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import EditGenAiTask from "./EditGenAiTask";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { userEvent } from "@storybook/test";

export default {
    title: "Pages/Tasks/Ongoing Tasks/Edit tasks/GenAI",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/OvnqnwTMCiznsaTUxZHiP6/Pages---Gen-AI",
        },
    },
} satisfies Meta;

export const Basic: StoryObj = {
    render: () => {
        const { cluster } = mockStore;
        const { tasksService } = mockServices;

        cluster.with_Single();
        tasksService.withConnectionStrings();

        return (
            <div style={{ height: 700 }}>
                <EditGenAiTask />
            </div>
        );
    },
};

export const Context: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await userEvent.type(await canvas.findByLabelText("Task Name"), "some-name");
        await userEvent.click(canvas.getByText("Select..."));
        await userEvent.click(canvas.getByText("ai-name"));
        await userEvent.click(canvas.getByText("Next"));
    },
};
