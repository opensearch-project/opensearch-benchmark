#!/bin/bash
# Full Vespa setup for Amazon Linux (m5.4xlarge+ recommended)
# Run as ec2-user on a fresh instance
echo "=== Installing Docker ==="
sudo yum update -y
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user

echo "=== Starting Vespa container ==="
sudo docker run -d --name vespa \
  --restart unless-stopped \
  -p 8080:8080 -p 19071:19071 \
  vespaengine/vespa 2>/dev/null || echo "Container already exists, starting it..."
sudo docker start vespa 2>/dev/null || true

echo "=== Waiting for config server (may take 2-3 minutes) ==="
until sudo docker exec vespa bash -c 'curl -s localhost:19071/state/v1/health | grep -qi up' 2>/dev/null; do
  sleep 5
  echo "  still waiting..."
done
echo "Config server ready."

echo "=== Creating application package ==="
rm -rf ~/vespa-app ~/app.tar.gz
mkdir -p ~/vespa-app/schemas

cat > ~/vespa-app/hosts.xml <<'HOSTS_END'
<?xml version="1.0" encoding="utf-8" ?>
<hosts>
    <host name="localhost">
        <alias>node1</alias>
    </host>
</hosts>
HOSTS_END

cat > ~/vespa-app/services.xml <<'SERVICES_END'
<?xml version="1.0" encoding="utf-8" ?>
<services version="1.0" xmlns:deploy="vespa" xmlns:preprocess="properties">
    <container id="default" version="1.0">
        <search/>
        <document-api/>
        <nodes>
            <node hostalias="node1"/>
        </nodes>
    </container>

    <content id="content" version="1.0">
        <redundancy>1</redundancy>
        <documents>
            <document type="big5" mode="index"/>
        </documents>
        <nodes>
            <node hostalias="node1" distribution-key="0"/>
        </nodes>
    </content>
</services>
SERVICES_END

cat > ~/vespa-app/schemas/big5.sd <<'SCHEMA_END'
schema big5 {
    document big5 {
        field timestamp type long {
            indexing: summary | attribute
            attribute: fast-search
        }
        field message type string {
            indexing: summary | index
            index: enable-bm25
        }
        field metrics_size type long {
            indexing: summary | attribute
        }
        field metrics_tmin type long {
            indexing: summary | attribute
        }
        field agent_ephemeral_id type string {
            indexing: summary | attribute
        }
        field agent_id type string {
            indexing: summary | attribute
        }
        field agent_name type string {
            indexing: summary | attribute
        }
        field agent_type type string {
            indexing: summary | attribute
        }
        field agent_version type string {
            indexing: summary | attribute
        }
        field aws_cloudwatch_ingestion_time type string {
            indexing: summary | attribute
        }
        field aws_cloudwatch_log_group type string {
            indexing: summary | attribute
        }
        field aws_cloudwatch_log_stream type string {
            indexing: summary | attribute
        }
        field cloud_region type string {
            indexing: summary | attribute
        }
        field data_stream_dataset type string {
            indexing: summary | attribute
        }
        field data_stream_namespace type string {
            indexing: summary | attribute
        }
        field data_stream_type type string {
            indexing: summary | attribute
        }
        field ecs_version type string {
            indexing: summary | attribute
        }
        field event_dataset type string {
            indexing: summary | attribute
        }
        field event_id type string {
            indexing: summary | attribute
        }
        field event_ingested type string {
            indexing: summary | attribute
        }
        field input_type type string {
            indexing: summary | attribute
        }
        field log_file_path type string {
            indexing: summary | attribute
        }
        field meta_file type string {
            indexing: summary | attribute
        }
        field process_name type string {
            indexing: summary | attribute
        }
        field tags type string {
            indexing: summary | attribute
        }
    }

    fieldset default {
        fields: message
    }

    rank-profile default {
        first-phase {
            expression: attribute(timestamp)
        }
    }
}
SCHEMA_END

echo "=== Deploying application ==="
cd ~/vespa-app
tar -czf ~/app.tar.gz .
cd ~
curl -s --header "Content-Type: application/x-gzip" --data-binary @app.tar.gz http://localhost:19071/application/v2/tenant/default/prepareandactivate
echo ""

echo "=== Waiting for application to be ready ==="
until curl -s http://localhost:8080/ApplicationStatus 2>/dev/null | grep -q '"code"'; do
  sleep 5
  echo "  still waiting..."
done

echo ""
echo "=== Vespa is ready! ==="
curl -s http://localhost:8080/ApplicationStatus | python3 -m json.tool | head -10
echo ""
PRIVATE_IP=$(hostname -I | awk '{print $1}')
echo "Run benchmark with: --target-hosts=${PRIVATE_IP}:8080"
