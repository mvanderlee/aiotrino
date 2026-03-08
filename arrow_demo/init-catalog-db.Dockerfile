FROM alpine:3.19

RUN apk add --no-cache sqlite

COPY scripts/init-catalog-db.sh /init.sh

CMD ["sh", "/init.sh"]
