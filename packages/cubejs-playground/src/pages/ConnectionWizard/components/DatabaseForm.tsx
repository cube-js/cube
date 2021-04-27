import { Button, Form, Input, Space } from 'antd';
import { useEffect } from 'react';

import Base64Upload from './Base64Upload';

export default function DatabaseForm({
  db,
  deployment,
  loading = false,
  disabled = false,
  hostname = '',
  onSubmit,
  onCancel,
}) {
  const [form] = Form.useForm();

  useEffect(() => {
    form.setFieldsValue({ CUBEJS_DB_HOST: hostname });
  }, [hostname]);

  const defaultValues = (deployment.envVariables || []).reduce(
    (obj, envVar) => {
      obj[envVar.name] = envVar.value;
      return obj;
    },
    {}
  );

  return (
    <Form
      data-testid="wizard-db-form"
      form={form}
      layout="vertical"
      onFinish={(v) => {
        v['CUBEJS_DB_TYPE'] = db.driver;
        onSubmit(v);
      }}
      initialValues={defaultValues}
    >
      {db.settings.map((param) =>
        param.type === 'base64upload' ? (
          <Base64Upload
            onInput={({ raw, encoded }) => {
              if (param.uploadTarget) {
                form.setFieldsValue({ [param.uploadTarget]: encoded });
              }
              if (param.extractField) {
                form.setFieldsValue({
                  [param.extractField.formField]:
                    raw[param.extractField.jsonField],
                });
              }
            }}
          />
        ) : (
          <Form.Item
            key={param.env}
            label={param.title || param.env}
            name={param.env}
          >
            {param.title ? (
              <Input data-testid={param.env} />
            ) : (
              <Input.TextArea
                data-testid={param.env}
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

      <Button
        data-testid="wizard-form-submit-btn"
        type="primary"
        htmlType="submit"
        loading={loading}
        disabled={disabled}
      >
        Apply
      </Button>
    </Form>
  );
}
