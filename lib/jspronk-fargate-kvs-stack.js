const cdk = require('@aws-cdk/core');
const lambda = require('@aws-cdk/aws-lambda');
const ecs = require('@aws-cdk/aws-ecs');
const ecsPatterns = require('@aws-cdk/aws-ecs-patterns');
const ec2 = require('@aws-cdk/aws-ec2');
const sqs = require('@aws-cdk/aws-sqs');
const s3 = require('@aws-cdk/aws-s3');
const iam = require('@aws-cdk/aws-iam')
const path = require('path');

class JspronkFargateKvsStack extends cdk.Stack {
  /**
   *
   * @param {cdk.Construct} scope
   * @param {string} id
   * @param {cdk.StackProps=} props
   */
  constructor(scope, id, props) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'recordingQueue', {});
    const bucket = new s3.Bucket(this, 'recordings');
    const recordingTrigger = new lambda.Function(this, 'recordingTrigger', {
      code: lambda.Code.fromAsset(path.resolve(__dirname, '../src/recordingTrigger')),
      handler: 'lambda_function.lambda_handler',
      runtime: lambda.Runtime.PYTHON_3_8,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        QUEUE_URL: queue.queueUrl,
        BUCKET: bucket.bucketName
      }
    });

    queue.grantSendMessages(recordingTrigger);
    
    const vpc = new ec2.Vpc(this, 'recordingVPC')
    const cluster = new ecs.Cluster(this, 'recordingCluster', { vpc })
    const image = ecs.ContainerImage.fromAsset(path.resolve(__dirname, '../src/recordingAgent'))

    const fargateRecorderPool = new ecsPatterns.QueueProcessingFargateService(this, 'recorderPool', {
      cluster,
      cpu: 1024,
      memoryLimitMiB: 4096,
      desiredTaskCount: 1,
      maxScalingCapacity: 5,
      image,
      queue,
      environment: {
        'MAX_THREADS': '30',
        'APP_REGION': 'us-west-2',
        'RECORDINGS_BUCKET_NAME': bucket.bucketName,
        'RECORDINGS_KEY_PREFIX': '',
        'RECORDINGS_PUBLIC_READ_ACL': 'FALSE',
        'START_SELECTOR_TYPE': 'FRAGMENT_NUMBER'
      }
    })
    queue.grantConsumeMessages(fargateRecorderPool.service.taskDefinition.taskRole)
    const taskRole = fargateRecorderPool.service.taskDefinition.taskRole
    taskRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonKinesisVideoStreamsReadOnlyAccess')
    )
    bucket.grantReadWrite(taskRole)

    // add x-ray daemon to containers
    const xray = fargateRecorderPool.service.taskDefinition.addContainer('xray-daemon', {
      image: ecs.ContainerImage.fromRegistry('amazon/aws-xray-daemon'),
      cpu: 32,
      memoryReservationMiB: 256,
      logging: new ecs.AwsLogDriver({
        streamPrefix: 'xrayLog',
      }),
      essential: false
    })
    xray.taskDefinition.taskRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXrayDaemonWriteAccess')
    )
    xray.addPortMappings({
      containerPort: 2000,
      protocol: ecs.Protocol.UDP,
    })
  }
}

module.exports = { JspronkFargateKvsStack }
