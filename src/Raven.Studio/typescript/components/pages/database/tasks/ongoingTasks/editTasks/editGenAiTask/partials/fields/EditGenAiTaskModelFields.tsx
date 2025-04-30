import { FormAceEditor } from "components/common/Form";
import { FormLabel } from "components/common/Form";
import { FormGroup } from "components/common/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { useFormContext } from "react-hook-form";
import OptionalLabel from "components/common/OptionalLabel";

export default function EditGenAiTaskModelFields() {
    const { control } = useFormContext();

    return (
        <>
            <FormGroup>
                <FormLabel>Prompt</FormLabel>
                <FormAceEditor control={control} name="prompt" mode="plain_text" />
            </FormGroup>
            <Row>
                <Col>
                    <FormGroup>
                        <FormLabel>Sample Object</FormLabel>
                        <FormAceEditor control={control} name="sampleObject" mode="json" />
                    </FormGroup>
                </Col>
                <Col>
                    <FormGroup>
                        <FormLabel>
                            JSON Schema <OptionalLabel />
                        </FormLabel>
                        <FormAceEditor control={control} name="jsonSchema" mode="json" />
                    </FormGroup>
                </Col>
            </Row>
        </>
    );
}
