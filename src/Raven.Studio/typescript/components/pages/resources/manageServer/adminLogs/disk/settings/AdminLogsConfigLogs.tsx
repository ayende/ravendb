import {
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Button,
    Col,
    Form,
    FormGroup,
    Label,
    Row,
    Table,
} from "reactstrap";
import * as yup from "yup";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { FormCheckbox, FormSelect } from "components/common/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { logFilterActionOptions, logLevelOptions, tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import { yupResolver } from "@hookform/resolvers/yup";
import AdminLogsFilterField from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsFilterField";
import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";
import { adminLogsUtils } from "components/pages/resources/manageServer/adminLogs/common/adminLogsUtils";

export default function AdminLogsConfigLogs({ targetId }: { targetId: string }) {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.Logs;
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { control, formState, handleSubmit, reset } = useForm<AdminLogsConfigLogsFormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const filterFieldArray = useFieldArray({
        control,
        name: "filters",
    });

    const handleSave: SubmitHandler<AdminLogsConfigLogsFormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveAdminLogsConfiguration(mapToDto(data));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <AccordionItem className="p-1 rounded-3">
            <AccordionHeader targetId={targetId}>Logs</AccordionHeader>
            <AccordionBody accordionId={targetId}>
                <h5 className="text-center text-muted text-uppercase">Writable</h5>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <Row>
                        <Col>
                            <FormGroup>
                                <Label>Current Minimum Level</Label>
                                <FormSelect control={control} name="minLevel" options={logLevelOptions} />
                                {!isCloud && (
                                    <FormCheckbox control={control} name="isPersist" className="mt-1">
                                        Save level in <code>settings.json</code>
                                        <AdminLogsPersistInfoIcon />
                                    </FormCheckbox>
                                )}
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <Label>Filter Default Action</Label>
                                <FormSelect
                                    control={control}
                                    name="logFilterDefaultAction"
                                    options={logFilterActionOptions}
                                />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup className="vstack">
                        <Label>Filters</Label>
                        <div className="vstack gap-1 mb-1">
                            {filterFieldArray.fields.map((field, idx) => (
                                <AdminLogsFilterField
                                    key={field.id}
                                    control={control}
                                    idx={idx}
                                    remove={() => filterFieldArray.remove(idx)}
                                />
                            ))}
                        </div>
                        <Button
                            type="button"
                            color="info"
                            className="w-fit-content"
                            onClick={() => filterFieldArray.append(adminLogsUtils.initialFilter)}
                        >
                            <Icon icon="plus" />
                            Add Filter
                        </Button>
                    </FormGroup>
                    <ButtonWithSpinner
                        color="success"
                        type="submit"
                        className="ms-auto"
                        icon="save"
                        isSpinning={formState.isSubmitting}
                        disabled={!formState.isDirty}
                    >
                        Save
                    </ButtonWithSpinner>
                </Form>
                <h5 className="text-center text-muted text-uppercase">Read-only</h5>
                <Table className="m-0">
                    <tbody>
                        <tr>
                            <td>Path</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Path} />
                            </td>
                        </tr>
                        <tr>
                            <td>Minimum Level</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MinLevel} />
                            </td>
                        </tr>
                        <tr>
                            <td>Archive Above Size</td>
                            <td>
                                <AdminLogsConfigTableValue
                                    value={`${config.ArchiveAboveSizeInMb?.toLocaleString()} MB`}
                                />
                            </td>
                        </tr>
                        <tr>
                            <td>Maximum Archived Days</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveDays} />
                            </td>
                        </tr>
                        <tr>
                            <td>Maximum Archived Files</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveFiles} />
                            </td>
                        </tr>
                        <tr>
                            <td>Archive File Compression</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.EnableArchiveFileCompression} />
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </AccordionBody>
        </AccordionItem>
    );
}

const schema = yup.object({
    minLevel: yup.string<Sparrow.Logging.LogLevel>().nullable().required(),
    isPersist: yup.boolean(),
    logFilterDefaultAction: yup.string<Sparrow.Logging.LogFilterAction>().nullable().required(),
    filters: adminLogsUtils.filtersSchema,
});

export type AdminLogsConfigLogsFormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(
    config: Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult["Logs"]
): AdminLogsConfigLogsFormData {
    return {
        minLevel: config.CurrentMinLevel,
        isPersist: false,
        filters: config.CurrentFilters.map((x) => ({
            minLevel: x.MinLevel,
            maxLevel: x.MaxLevel,
            condition: x.Condition,
            action: x.Action,
        })),
        logFilterDefaultAction: config.CurrentLogFilterDefaultAction,
    };
}

function mapToDto(
    data: AdminLogsConfigLogsFormData
): Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters> {
    return {
        Logs: {
            Filters: data.filters.map((x) => ({
                MinLevel: x.minLevel,
                MaxLevel: x.maxLevel,
                Condition: x.condition,
                Action: x.action,
            })),
            MinLevel: data.minLevel,
            LogFilterDefaultAction: data.logFilterDefaultAction,
        },
        Persist: data.isPersist,
    };
}
