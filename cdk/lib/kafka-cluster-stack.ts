import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface KafkaClusterStackProps extends cdk.StackProps {
 
  readonly kafkaVersion: string;
  readonly instanceType: string;
  
  readonly heapSize: string;
 
  readonly volumeSizeGb: number;
 
  readonly clientCidrs: string[];
}

const VPC_CIDR = '10.0.0.0/16';
const CLIENT_PORT = 9092;

const AMI_ID = 'ami-08be4b1b8afa29958';
const AMI_REGION = 'us-east-2';

export class KafkaClusterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: KafkaClusterStackProps) {
    super(scope, id, props);

   
    const vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr(VPC_CIDR),
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [{ name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 }],
    });
    const subnet = vpc.publicSubnets[0];

    const sg = new ec2.SecurityGroup(this, 'KafkaSecurityGroup', {
      vpc,
      description: 'Single-node Kafka (broker + KRaft controller)',
      allowAllOutbound: true,
    });
    sg.addIngressRule(ec2.Peer.ipv4(vpc.vpcCidrBlock), ec2.Port.tcp(CLIENT_PORT), 'Kafka clients inside the VPC');
    for (const cidr of props.clientCidrs) {
      sg.addIngressRule(ec2.Peer.ipv4(cidr), ec2.Port.tcp(CLIENT_PORT), 'Kafka clients (extra CIDR)');
    }

    
    const role = new iam.Role(this, 'KafkaRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')],
    });

    
    const clusterId: string =
      this.node.tryGetContext('clusterId') ??
      crypto.createHash('md5').update(`${this.account}/${this.region}/${this.node.addr}`).digest('base64url');

    const script = fs
      .readFileSync(path.join(__dirname, '..', 'assets', 'bootstrap.sh'), 'utf8')
      .replace('__KAFKA_VERSION__', props.kafkaVersion)
      .replace('__CLUSTER_ID__', clusterId)
      .replace('__HEAP_SIZE__', props.heapSize);

    const instance = new ec2.Instance(this, 'KafkaInstance', {
      vpc,
      vpcSubnets: { subnets: [subnet] },
      instanceType: new ec2.InstanceType(props.instanceType),
      
      machineImage: ec2.MachineImage.genericLinux({ [AMI_REGION]: AMI_ID }),
      securityGroup: sg,
      role,
      requireImdsv2: true,
      userData: ec2.UserData.custom(script),
      userDataCausesReplacement: true,
      blockDevices: [
        {
          deviceName: '/dev/xvda',
          volume: ec2.BlockDeviceVolume.ebs(props.volumeSizeGb, {
            volumeType: ec2.EbsDeviceVolumeType.GP3,
            encrypted: true,
          }),
        },
      ],
    });
    
    instance.node.addDependency(subnet.internetConnectivityEstablished);
    cdk.Tags.of(instance).add('Name', `${this.stackName}-kafka`);

    new cdk.CfnOutput(this, 'BootstrapServers', {
      description: 'Kafka bootstrap server (reachable from inside the VPC).',
      value: `${instance.instancePrivateIp}:${CLIENT_PORT}`,
    });
    new cdk.CfnOutput(this, 'InstanceId', {
      description: 'EC2 instance ID, for `aws ssm start-session --target <id>`.',
      value: instance.instanceId,
    });
    new cdk.CfnOutput(this, 'ClusterId', { value: clusterId });
    new cdk.CfnOutput(this, 'VpcId', { value: vpc.vpcId });
  }
}