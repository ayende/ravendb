import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import EditGenAiTask from "./EditGenAiTask";
import { mockStore } from "test/mocks/store/MockStore";

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

export const Default: StoryObj = {
    name: "GenAI",
    render: () => {
        const { cluster } = mockStore;
        cluster.with_Single();

        return <EditGenAiTask />;
    },
};
