#!/bin/bash

CLUSTER_PASSWORD=$1
CLUSTER_VERSION=$2
CLUSTER_ARCH=$3
#OS_SNAPSHOT_AWS_ACCESS_KEY_ID=$4
#OS_SNAPSHOT_AWS_SECRET_ACCESS_KEY=$5
#CLUSTER_IPS=$6
#NODE_NAME=$7
#NODES_TYPE=$8
CLUSTER_IPS=$4
NODE_NAME=$5
NODES_TYPE=$6

# Check if the cluster version is a nightly one
if [[ $CLUSTER_VERSION == *"-nightly-"* ]]; then
    IS_NIGHTLY=true
    NIGHTLY_VERSION="${CLUSTER_VERSION#*-nightly-}"
    CLUSTER_VERSION="${CLUSTER_VERSION/-nightly*/}"
    echo "Downloading nightly version $CLUSTER_VERSION of OpenSearch"
else
    echo "Downloading version $CLUSTER_VERSION of OpenSearch"
fi

INSTALL_ROOT=/mnt/opensearch
INSTALL_PATH=$INSTALL_ROOT/opensearch-$CLUSTER_VERSION
INSTALL_FILENAME=opensearch-$CLUSTER_VERSION-linux-$CLUSTER_ARCH.tar.gz
# If it's a nightly version, download it from the nightly repository
if [[ $IS_NIGHTLY == true ]]; then
    DOWNLOAD_URL=https://ci.opensearch.org/ci/dbc/distribution-build-opensearch/$CLUSTER_VERSION/$NIGHTLY_VERSION/linux/$CLUSTER_ARCH/tar/dist/opensearch/$INSTALL_FILENAME

    S3_INSTALL_FILENAME=repository-s3-$CLUSTER_VERSION.zip
    S3_PLUGIN_URL=https://ci.opensearch.org/ci/dbc/distribution-build-opensearch/$CLUSTER_VERSION/$NIGHTLY_VERSION/linux/$CLUSTER_ARCH/tar/builds/opensearch/core-plugins/$S3_INSTALL_FILENAME
else
    DOWNLOAD_URL=https://artifacts.opensearch.org/releases/bundle/opensearch/$CLUSTER_VERSION/$INSTALL_FILENAME
fi
CONFIG_FILE=$INSTALL_PATH/config/opensearch.yml
JVM_CONFIG=$INSTALL_PATH/config/jvm.options

cd /mnt || exit 1

# Download and install OpenSearch then remove installer
mkdir -p $INSTALL_PATH
wget $DOWNLOAD_URL
tar -xvf $INSTALL_FILENAME -C $INSTALL_ROOT
rm $INSTALL_FILENAME

# Specify directories for storage and update the configuration to allow incoming connections.
# Also a config that is needed to make the s3 client successfully locate the snapshot bucket
cat <<EOF > $CONFIG_FILE
network.host: 0.0.0.0
node.name: $NODE_NAME
path.repo: ["/mnt/backup"]
path.data: /mnt/data
path.logs: /mnt/logs
s3.client.default.region: us-east-1
indices.recovery.max_bytes_per_sec: 2048mb
search.concurrent_segment_search.mode: auto
EOF

if [[ "$NODES_TYPE" == "multi" ]]; then
    # multi-node settings
    cat <<EOF >> $CONFIG_FILE
cluster.initial_cluster_manager_nodes: main-node
discovery.seed_hosts: [$CLUSTER_IPS]
EOF
else
    # single node settings
    echo "discovery.type: single-node" >> $CONFIG_FILE
fi

# Replace the JVM options file with the correct one for the OS version
CURRENT_OS_VERSION=$(echo "$CLUSTER_VERSION" | cut -d. -f1)
JVM_VERSION_CONFIG="/jvm.v$CURRENT_OS_VERSION.options"
echo "Copying JVM options file $JVM_VERSION_CONFIG to $JVM_CONFIG"
cp $JVM_VERSION_CONFIG $JVM_CONFIG

sudo mkdir /mnt/backup && sudo chmod ugo+rwx /mnt/backup
sudo mkdir /mnt/data && sudo chmod ugo+rwx /mnt/data
sudo mkdir /mnt/logs && sudo chmod ugo+rwx /mnt/logs


# JDK location
export OPENSEARCH_JAVA_HOME=$INSTALL_PATH/jdk
echo "export OPENSEARCH_JAVA_HOME=$OPENSEARCH_JAVA_HOME" >> ~/.bashrc

# Fix the JVM size
GB=$(echo "$(cat /proc/meminfo | grep MemTotal | awk '{print $2}') / (1024*1024*2)" | bc)
sed -i "s/-Xms1g/-Xms${GB}g/" $JVM_CONFIG
sed -i "s/-Xmx1g/-Xmx${GB}g/" $JVM_CONFIG

# Install the s3 plugin if necessary (S3_PLUGIN_URL and S3_INSTALL_FILENAME is set)
if [[ -n "$S3_PLUGIN_URL" ]]; then
    wget $S3_PLUGIN_URL
    sudo $INSTALL_PATH/bin/opensearch-plugin install -b file:///mnt/$S3_INSTALL_FILENAME
else
    sudo $INSTALL_PATH/bin/opensearch-plugin install -b -s repository-s3
fi

if [[ "$CURRENT_OS_VERSION" == "2" ]]; then
    # Manually run security demo config to modify it
    OPENSEARCH_INITIAL_ADMIN_PASSWORD=$CLUSTER_PASSWORD bash $INSTALL_PATH/plugins/opensearch-security/tools/install_demo_configuration.sh -y -i -s || exit 1
    # Set allowed TLS protocols to fix: https://github.com/opensearch-project/security/issues/3299
    echo 'plugins.security.ssl.http.enabled_protocols: ["TLSv1.2"]' >> $CONFIG_FILE
fi

# Run opensearch startup script with security demo configuration
OPENSEARCH_INITIAL_ADMIN_PASSWORD=$CLUSTER_PASSWORD $INSTALL_PATH/opensearch-tar-install.sh &> opensearch.log &
SERVER_PID=$!

# Record the pid
echo $SERVER_PID > /mnt/pid

echo "Waiting for server to boot"
# Wait for OpenSearch to start (break after 20 tries)
tries=0
while ! curl --max-time 5 -ks https://localhost:9200 > /dev/null 2>&1 ; do
    echo "Waiting for OpenSearch to start ($tries)"
    ((tries++))
    sleep $tries
    if [ $tries -eq 20 ]; then
        echo "Failed to start OpenSearch"
        exit 1
    fi
done 

echo "OpenSearch responds on port 9200, now verify credentials"
curl -X GET https://localhost:9200 -u "admin:$CLUSTER_PASSWORD" --insecure || (echo "Failed to query server" && false)
echo
echo "Server up and running (pid $SERVER_PID)"