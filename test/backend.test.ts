import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { BackendStack } from '../lib/backend-stack';

test('WebSocket API and RDS PostgreSQL instance are defined', () => {
  const app = new cdk.App();
  const stack = new BackendStack(app, 'TestStack', {
    env: { account: '123456789012', region: 'us-east-1' },
  });
  const template = Template.fromStack(stack);
  template.resourceCountIs('AWS::ApiGatewayV2::Api', 1);
  template.resourceCountIs('AWS::RDS::DBInstance', 1);
});
