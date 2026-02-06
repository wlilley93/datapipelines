FROM public.ecr.aws/aws-cli/aws-cli:2.15.0

ARG TARGETARCH

RUN set -e; \
    case "$TARGETARCH" in \
      arm64) smp_arch="linux_arm64" ;; \
      amd64) smp_arch="linux_64bit" ;; \
      *) echo "Unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac; \
    curl -fsSL -o /tmp/session-manager-plugin.rpm "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/${smp_arch}/session-manager-plugin.rpm"; \
    yum install -y /tmp/session-manager-plugin.rpm socat; \
    rm -f /tmp/session-manager-plugin.rpm; \
    yum clean all

COPY infrastructure/ssm-tunnel/start.sh /usr/local/bin/start-ssm-tunnel
RUN chmod +x /usr/local/bin/start-ssm-tunnel

ENTRYPOINT ["/usr/local/bin/start-ssm-tunnel"]
