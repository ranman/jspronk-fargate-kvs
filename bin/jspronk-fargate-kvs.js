#!/usr/bin/env node

const cdk = require('@aws-cdk/core');
const { JspronkFargateKvsStack } = require('../lib/jspronk-fargate-kvs-stack');

const app = new cdk.App();
new JspronkFargateKvsStack(app, 'JspronkFargateKvsStack');
