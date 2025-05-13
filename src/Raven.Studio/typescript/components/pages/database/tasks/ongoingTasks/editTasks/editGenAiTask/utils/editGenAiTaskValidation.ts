import * as yup from "yup";

type EditGenAiTaskSchemaProvider = "jsonSchema" | "sampleObject";

export const editGenAiTaskSchema = yup.object({
    name: yup.string().required(),
    state: yup.string<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup.string().nullable(),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().required(),
    isAllowEtlOnNonEncryptedChannel: yup.boolean(),
    collectionName: yup.string().required(),
    schemaProvider: yup.string<EditGenAiTaskSchemaProvider>().nullable().required(),
    prompt: yup.string().required(),
    jsonSchema: yup.string(),
    sampleObject: yup.string(),
    update: yup.string().required(),
    isResetScript: yup.boolean(),
    scriptToReset: yup.string().nullable(),
    script: yup.string().required(),
    // For testing
    documentId: yup.string(),
    playgroundDocument: yup.string(),
    playgroundContexts: yup.array().of(yup.object({ value: yup.string() })),
    playgroundModelOutputs: yup.array().of(yup.object({ value: yup.string() })),
});

export type EditGenAiTaskFormData = yup.InferType<typeof editGenAiTaskSchema>;
