import { Button, Form, Input, Space } from 'antd';
// import { Base64Upload } from '@cubejs-enterprise/uikit';

export default function DatabaseForm({
  db,
  deployment,
  loading = false,
  disabled = false,
  onSubmit,
  onCancel,
}) {
  const [form] = Form.useForm();

  const defaultValue = (deployment.envVariables || []).reduce((obj, envVar) => {
    obj[envVar.name] = envVar.value;
    return obj;
  }, {});

  return (
    <Form
      form={form}
      layout="vertical"
      onFinish={(v) => {
        v['CUBEJS_DB_TYPE'] = db.driver;
        onSubmit(v);
      }}
      initialValues={defaultValue}
    >
      {db.settings.map((param) =>
        param.type === 'base64upload' ? (
          <div>Base64Upload</div>
        ) : (
          // <Base64Upload
          //   key={param.env}
          //   width="100%"
          //   margin="2x bottom"
          //   accept="application/json, .json"
          //   onInput={({ raw, encoded }) => {
          //     if (param.uploadTarget) {
          //       form.setFieldsValue({ [param.uploadTarget]: encoded });
          //     }
          //     if (param.extractField) {
          //       form.setFieldsValue({
          //         [param.extractField.formField]:
          //           raw[param.extractField.jsonField],
          //       });
          //     }
          //   }}
          // />
          <Form.Item
            key={param.env}
            label={param.title || param.env}
            name={param.env}
          >
            {param.title ? (
              <Input />
            ) : (
              <Input.TextArea
                rows={1}
                style={{
                  overflow: 'hidden',
                  resize: 'none',
                }}
              />
            )}
          </Form.Item>
        )
      )}
      <Space>
        <Button
          type="primary"
          htmlType="submit"
          loading={loading}
          disabled={disabled}
        >
          Apply
        </Button>

        <Button onClick={onCancel} data-qa="SetUpLater">
          Set Up Later
        </Button>
      </Space>
    </Form>
  );
}
