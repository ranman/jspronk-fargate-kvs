const { expect, matchTemplate, MatchStyle } = require('@aws-cdk/assert');
const cdk = require('@aws-cdk/core');
const JspronkFargateKvs = require('../lib/jspronk-fargate-kvs-stack');

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new JspronkFargateKvs.JspronkFargateKvsStack(app, 'MyTestStack');
    // THEN
    expect(stack).to(matchTemplate({
      "Resources": {}
    }, MatchStyle.EXACT))
});