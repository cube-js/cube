FROM rustembedded/cross:x86_64-pc-windows-gnu

RUN apt-get update && \
    apt-get install -y libssl1.1 curl
