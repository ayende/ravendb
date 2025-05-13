import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import EditGenAiTask from "./EditGenAiTask";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import { userEvent } from "@storybook/test";
import { EditGenAiTaskStepId } from "./hooks/useEditGenAiTaskSteps";
import { Canvas } from "storybook/internal/csf";

export default {
    title: "Pages/Tasks/Ongoing Tasks/Edit tasks/GenAI",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/OvnqnwTMCiznsaTUxZHiP6/Pages---Gen-AI",
        },
        test: {
            dangerouslyIgnoreUnhandledErrors: true,
        },
    },
} satisfies Meta;

export const Basic: StoryObj = {
    render: () => {
        const { cluster, collectionsTracker } = mockStore;
        const { tasksService } = mockServices;

        cluster.with_Single();
        collectionsTracker.with_Collections();

        tasksService.withTestAiConnectionString();
        tasksService.withConnectionStrings();
        tasksService.withTestGenAi();

        return (
            <div style={{ height: 700 }}>
                <EditGenAiTask />
            </div>
        );
    },
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "basic");
    },
};

export const Context: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "context");
    },
};

export const ModelInput: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "modelInput");
    },
};

export const UpdateScript: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "updateScript");
    },
};

export const Summary: StoryObj = {
    ...Basic,
    play: async ({ canvas }) => {
        await navigateToStep(canvas, "summary");
    },
};

async function navigateToStep(canvas: Canvas, step: EditGenAiTaskStepId) {
    await userEvent.type(await canvas.findByLabelText("Task Name"), "some-name");
    await userEvent.click(canvas.getByText("Select..."));
    await userEvent.click(canvas.getByText("ai-name"));

    if (step === "basic") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("Select...")[0]);
    await userEvent.click(canvas.getByText("Orders"));
    await userEvent.click(canvas.getByText("1"));
    await userEvent.paste(sampleContextScript);
    await userEvent.click(canvas.getByRole("button", { name: "I want to provide content manually" }));
    await userEvent.click(canvas.getByRole("button", { name: "Enter edit mode" }));
    await userEvent.click(canvas.getByRole("button", { name: "Test task context" }));

    if (step === "context") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("1")[0]);
    await userEvent.paste(samplePrompt);
    await userEvent.click(canvas.getByText("Use sample object"));
    await userEvent.click(canvas.getAllByText("1")[1]);
    await userEvent.paste(sampleObject);
    await userEvent.click(canvas.getByRole("button", { name: "Test model" }));

    if (step === "modelInput") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
    await userEvent.click(canvas.getAllByText("1")[0]);
    await userEvent.paste(sampleUpdateScript);
    await userEvent.click(canvas.getByRole("button", { name: "Test update script" }));

    if (step === "updateScript") {
        return;
    }

    await userEvent.click(canvas.getByText("Next"));
}

const sampleContextScript = `for(const comment of this.Comments)
{
    context({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.AiHash);
}`;

const samplePrompt = "Check if the following blog post comment is spam or not";

const sampleObject = `{
    "Blocked": true,
    "Reason": "Concise reason for why this comment was marked as spam or ham"
}`;

const sampleUpdateScript = `const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
else 
{
    this.Comments[idx].AiHash = $aiHash; // remember this decision
}`;
