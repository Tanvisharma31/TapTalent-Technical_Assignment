import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as apigwv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import { Construct } from 'constructs';

export class BackendStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const region = cdk.Stack.of(this).region;
    const vpc = new ec2.Vpc(this, 'ChatVpc', {
      availabilityZones: [`${region}a`, `${region}b`],
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
    });

    // t3.micro fits Free Tier; public ingress lets Lambda (outside VPC) reach RDS without NAT.
    const db = new rds.DatabaseInstance(this, 'ChatDb', {
      engine: rds.DatabaseInstanceEngine.postgres({
        version: rds.PostgresEngineVersion.VER_16_4,
      }),
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      publiclyAccessible: true,
      credentials: rds.Credentials.fromGeneratedSecret('postgres'),
      databaseName: 'chatsystem',
      allocatedStorage: 20,
      maxAllocatedStorage: 50,
      storageEncrypted: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deletionProtection: false,
    });

    // Security group rule description: AWS allows only a limited ASCII set.
    db.connections.allowFrom(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(5432),
      'Postgres 5432 from any IPv4 for Lambda outside VPC demo only',
    );

    const wsHandler = new nodejs.NodejsFunction(this, 'WsHandler', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../lambda/websocket/handler.ts'),
      handler: 'handler',
      timeout: cdk.Duration.seconds(29),
      memorySize: 256,
      bundling: {
        minify: true,
        sourceMap: false,
        externalModules: [],
      },
      environment: {
        PGHOST: db.dbInstanceEndpointAddress,
        PGPORT: db.dbInstanceEndpointPort.toString(),
        PGDATABASE: 'chatsystem',
        SECRET_ARN: db.secret!.secretArn,
      },
    });

    db.secret!.grantRead(wsHandler);

    const webSocketApi = new apigwv2.WebSocketApi(this, 'ChatWs', {
      apiName: 'tap-talent-anonymous-chat',
      description: 'Anonymous random text chat WebSocket API',
      connectRouteOptions: {
        integration: new integrations.WebSocketLambdaIntegration('OnConnect', wsHandler),
      },
      disconnectRouteOptions: {
        integration: new integrations.WebSocketLambdaIntegration('OnDisconnect', wsHandler),
      },
    });

    const route = (id: string) =>
      new integrations.WebSocketLambdaIntegration(`Route${id}`, wsHandler);
    for (const key of ['search', 'message', 'skip', 'end', 'init'] as const) {
      webSocketApi.addRoute(key, { integration: route(key) });
    }
    webSocketApi.addRoute('$default', { integration: route('Default') });

    const stage = new apigwv2.WebSocketStage(this, 'ProdStage', {
      webSocketApi,
      stageName: 'prod',
      autoDeploy: true,
      // Caps steady-state + burst Lambda invocations at API layer (helps avoid surprise free-tier usage).
      throttle: {
        rateLimit: 40,
        burstLimit: 80,
      },
    });

    webSocketApi.grantManageConnections(wsHandler);

    new cdk.CfnOutput(this, 'WebSocketUrl', {
      value: stage.url,
      description: 'Connect the React app to this wss:// URL',
    });

    new cdk.CfnOutput(this, 'DbEndpoint', {
      value: db.dbInstanceEndpointAddress,
      description: 'PostgreSQL host (for optional SQL clients)',
    });

    new cdk.CfnOutput(this, 'DbSecretArn', {
      value: db.secret!.secretArn,
    });
  }
}
