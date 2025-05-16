import { FormAceEditor, FormLabel, FormGroup, FormValidationMessage } from "components/common/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import { ReactNode } from "react";
import IconName from "typings/server/icons";
import { HStack } from "components/common/utilities/HStack";
import EditGenAiLoadFile from "../EditGenAiLoadFile";

export default function EditGenAiTaskModelFields() {
    const {
        control,
        setValue,
        formState: { errors },
    } = useFormContext();
    const formValues = useWatch({ control });

    return (
        <>
            <FormGroup>
                <FormLabel>Prompt</FormLabel>
                <FormAceEditor control={control} name="prompt" mode="text" />
            </FormGroup>
            {formValues.schemaProvider == null && (
                <div>
                    <div className="mb-1">JSON schema</div>
                    <Row>
                        <Col>
                            <SchemaProviderButton
                                icon="default"
                                title={
                                    <>
                                        Use sample object <Badge bg="faded-success">Recommended</Badge>
                                    </>
                                }
                                description="Choose if you want to generate schema out of sample object"
                                handleClick={() => setValue("schemaProvider", "sampleObject")}
                            />
                        </Col>
                        <Col>
                            <SchemaProviderButton
                                icon="edit"
                                title="Provide manually"
                                description="Choose if you want to manually provide the schema"
                                handleClick={() => setValue("schemaProvider", "jsonSchema")}
                            />
                        </Col>
                    </Row>
                    {errors.schemaProvider && (
                        <FormValidationMessage>{errors.schemaProvider?.message.toString()}</FormValidationMessage>
                    )}
                </div>
            )}
            {formValues.schemaProvider === "sampleObject" && (
                <FormGroup>
                    <FormLabel className="hstack justify-content-between">
                        <div>Sample Object</div>
                        <HStack gap={2}>
                            <EditGenAiLoadFile name="sampleObject" />
                            <Button variant="link" size="xs" onClick={() => setValue("schemaProvider", "jsonSchema")}>
                                <Icon icon="edit" />
                                Provide manually
                            </Button>
                        </HStack>
                    </FormLabel>
                    <FormAceEditor control={control} name="sampleObject" mode="json" />
                </FormGroup>
            )}
            {formValues.schemaProvider === "jsonSchema" && (
                <FormGroup>
                    <FormLabel className="hstack justify-content-between">
                        <div>JSON Schema</div>
                        <HStack gap={2}>
                            <EditGenAiLoadFile name="jsonSchema" />
                            <Button variant="link" size="xs" onClick={() => setValue("schemaProvider", "sampleObject")}>
                                <Icon icon="default" />
                                Use sample object
                            </Button>
                        </HStack>
                    </FormLabel>
                    <FormAceEditor control={control} name="jsonSchema" mode="json" />
                </FormGroup>
            )}
        </>
    );
}

interface SchemaProviderButtonProps {
    icon: IconName;
    title: ReactNode;
    description: ReactNode;
    handleClick: () => void;
}

function SchemaProviderButton({ icon, title, description, handleClick }: SchemaProviderButtonProps) {
    return (
        <div className="border border-secondary rounded p-2 cursor-pointer h-100" onClick={handleClick}>
            <div className="text-emphasis hstack gap-2 h-100">
                <div>
                    <Icon icon={icon} margin="m-0" style={{ fontSize: 24 }} />
                </div>
                <div className="flex-grow">
                    <h4 className="mb-1">{title}</h4>
                    <span>{description}</span>
                </div>
            </div>
        </div>
    );
}
