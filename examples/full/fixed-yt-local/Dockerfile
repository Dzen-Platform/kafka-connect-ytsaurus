FROM ytsaurus/local:stable

RUN rm -f /usr/bin/start.sh

COPY start.sh /usr/bin
RUN chmod +x /usr/bin/start.sh

ENTRYPOINT ["bash", "/usr/bin/start.sh"]