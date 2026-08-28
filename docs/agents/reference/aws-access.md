# Reference: AWS access patterns

Generic patterns for working with AWS resources from an agent session. Specifics (account IDs, instance IDs, profile names, IP ranges) belong in the user's local notes, not here.

## Production safety

Before doing **anything** in AWS:

1. **Confirm which account you're operating in.**
   ```
   aws sts get-caller-identity --profile <profile>
   ```
   The output includes the account ID. Verify against the account the user specified.

2. **Prefer read-only over write.** `describe`, `list`, `get` first. Only mutate with explicit user direction.

3. **Never tear down infrastructure you didn't create** without explicit user permission. EC2 instances, security groups, IAM roles, CloudFormation stacks: if you didn't make it, it's likely someone else's work in progress.

4. **Assume production when uncertain.** If you can't tell whether a resource is prod or test, treat it as prod and don't touch it.

5. **Destructive operations** (`terminate-instances`, `delete-stack`, `iam delete-role`, `cloudformation destroy`) require explicit user confirmation in the current session. A user permitting them once does not authorize them in future sessions.

## Refreshing credentials

Before assuming credentials are missing, diagnose:

```
aws sts get-caller-identity                        # account + role currently in use
echo "${AWS_PROFILE:-default}"                     # which profile is active
cat ~/.aws/config 2>/dev/null | head              # what profiles are configured
```

Then use whatever credential mechanism the environment provides: `aws configure sso`, `aws-vault`, IAM Identity Center, named profiles in `~/.aws/credentials`, environment variables, or an organization-specific helper. The skill files in this directory assume the caller has already obtained valid credentials.

If `aws sts get-caller-identity` returns `ExpiredToken` mid-session, refresh and retry the failed command. Never hard-code access keys or session tokens in scripts.

## Accessing EC2 hosts

Two common patterns. SSM is preferred for ephemeral access; SSH is preferred for long-running interactive sessions.

### SSM (preferred for one-shots)

The host must have the SSM agent running and an instance profile with `AmazonSSMManagedInstanceCore`. Most modern AMIs satisfy this.

**Interactive shell:**
```
aws ssm start-session --target <instance-id> --region <region> --profile <profile>
```

**One-shot command:**
```
aws ssm send-command \
  --profile <profile> --region <region> \
  --instance-ids <instance-id> \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["<command-here>"]' \
  --query 'Command.CommandId' --output text
```

This returns a command ID. Poll for the result:

```
aws ssm get-command-invocation \
  --profile <profile> --region <region> \
  --command-id <command-id> --instance-id <instance-id> \
  --query '[Status,StandardOutputContent,StandardErrorContent]' --output text
```

Status progresses `Pending → InProgress → Success` (or `Failed`/`TimedOut`). Output truncates around 24 KB: for longer output, redirect to a file on the host and read it back.

### SSM gotchas

- **Quoting:** `send-command` parses the `commands` array as JSON, then the host shell parses it again. Complex commands with nested quotes, parens, ampersands, or heredocs break in opaque ways.
- **Escape hatch for scripts that resist JSON escaping:** base64-encode the script and decode on the host. Use this only when the script is one-shot. For anything reused, upload it to S3 (or bake it into the AMI/user-data) and invoke by path instead.
  ```
  SCRIPT='<your multi-line shell script>'
  B64=$(echo "$SCRIPT" | base64 | tr -d '\n')
  aws ssm send-command \
    --instance-ids <instance-id> \
    --document-name AWS-RunShellScript \
    --parameters "{\"commands\":[\"echo $B64 | base64 -d > /tmp/script.sh && bash /tmp/script.sh\"]}" \
    ...
  ```
- **Timeout:** default is 600 seconds. For long jobs (benchmark runs), set `executionTimeout`:
  ```
  --parameters '{"commands":["..."],"executionTimeout":["7200"]}'
  ```
- **Process groups:** background processes (`&`, `nohup`) started inside `send-command` are killed when the command returns. Use `screen`, `tmux`, or `systemd-run` to detach properly.

### SSH

If the host has an SSH key configured and the security group allows your IP:

```
ssh -i ~/.ssh/<key>.pem ec2-user@<public-ip-or-private-ip>
```

SSH preserves the connection through long commands and supports `scp`/`rsync` for file transfer. Prefer SSH when you'll be iterating interactively for more than a few commands.

## Security groups and reachability

A common failure mode is "OSB host can't connect to OS cluster": usually a security group issue.

Diagnose:
```
# From the OSB host:
nc -zv <target-host> <target-port>
# or
curl -v <target-host>:<target-port>
```

If unreachable, check:
1. Is the target's security group inbound rule allowing the OSB host's security group or CIDR on the right port?
2. Are both hosts in the same VPC, or connected via peering / transit gateway?
3. Is the target listening on the right interface (`network.host: 0.0.0.0` vs `127.0.0.1`)?

Don't open security groups to `0.0.0.0/0` to "make it work." Always narrow to the specific source.

## Credential and PII hygiene

- Never commit AWS access keys, session tokens, or `.aws/credentials` files.
- Strip them from logs before sharing.
- If a script needs credentials, read them from environment variables or named profiles, not hard-coded.

## Useful commands

```
# Which account am I in?
aws sts get-caller-identity --profile <profile>

# List my running instances
aws ec2 describe-instances --profile <profile> --region <region> \
  --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`Name`].Value|[0],PrivateIpAddress,InstanceType]' \
  --output table

# Find instances by tag
aws ec2 describe-instances --profile <profile> --region <region> \
  --filters "Name=tag:Name,Values=*benchmark*" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,LaunchTime]' --output table

# Get console output (useful when SSM/SSH unreachable)
aws ec2 get-console-output --profile <profile> --instance-id <id> --output text | tail -100
```
