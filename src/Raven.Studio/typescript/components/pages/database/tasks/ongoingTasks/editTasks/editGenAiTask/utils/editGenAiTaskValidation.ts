import * as yup from "yup";

type EditGenAiTaskSchemaProvider = "jsonSchema" | "sampleObject";

export const editGenAiTaskSchema = yup.object({
    name: yup.string().required(),
    identifier: yup.string(),
    state: yup.string<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>().required(),
    isSetResponsibleNode: yup.boolean(),
    responsibleNode: yup.string().nullable(),
    isPinResponsibleNode: yup.boolean(),
    connectionStringName: yup.string().required(),
    isAllowEtlOnNonEncryptedChannel: yup.boolean(),
    collectionName: yup.string().required(),
    prompt: yup.string().required(),
    schemaProvider: yup.string<EditGenAiTaskSchemaProvider>().nullable().required(),
    jsonSchema: yup.string().when("schemaProvider", {
        is: "jsonSchema",
        then: (schema) => schema.required(),
    }),
    sampleObject: yup.string().when("schemaProvider", {
        is: "sampleObject",
        then: (schema) => schema.required(),
    }),
    update: yup.string().required(),
    isForceSendingCachedObjects: yup.boolean(),
    isResetScript: yup.boolean(),
    script: yup.string().required(),

    // Playground
    documentId: yup.string(),
    playgroundDocument: yup.string(),
    playgroundContexts: yup.array().of(
        yup.object({
            idx: yup.number().nullable(),
            value: yup.string(),
            aiHash: yup.string(),
            isCached: yup.boolean(),
        })
    ),
    playgroundModelOutputs: yup.array().of(yup.object({ idx: yup.number().nullable(), value: yup.string() })),
});

export type EditGenAiTaskFormData = yup.InferType<typeof editGenAiTaskSchema>;
