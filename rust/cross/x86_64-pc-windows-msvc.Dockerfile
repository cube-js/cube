FROM madduci/docker-wine:5-stable

WORKDIR /home/wine/.wine/drive_c

USER root

#
#RUN echo "Extracting Visual C++ compiler" \
#    && curl -L http://172.17.0.1:20000/VC2019.zip -o VC.zip \
#    && mkdir -p Tools/VS2019 \
#    && unzip -d Tools/VS2019/ -q VC.zip; \
#    echo "Removing telemetry tool VCTIP from VC" \
#    && (find Tools/VS2019/ -type f -name 'VCTIP.exe' -exec rm -rf "{}" \; || echo "Done") \
#    && echo "Removing LLVM libraries from VC" \
#    && (find Tools/VS2019/ -type d -name 'Llvm' -exec rm -rf "{}" \;  || echo "Done") \
#    && echo "Removing Hostx86 folder from VC" \
#    && (find Tools/VS2019/ -type d -name 'Hostx86' -exec rm -rf "{}" \;  || echo "Done") \
#    && rm -rf VC.zip \
#    && echo "Extracting Windows SDK" \
#    && curl -L http://172.17.0.1:20000/SDK.zip -o SDK.zip \
#    && mkdir -p Tools/SDK \
#    && unzip -d Tools/SDK/ -q SDK.zip; \
#    echo "Removing arm libraries from SDK" \
#    && (find ./Tools/SDK -type d -name 'arm*' -exec rm -rf "{}" \;  || echo "Done") \
#    && echo "Removing old SDK versions" \
#    && (find ./Tools/SDK -type d -name '10.0.1[4-6]???.0' -exec rm -rf "{}" \;  || echo "Done") \
#    && rm -rf SDK.zip \
#    && echo "Extracting CMake" \
#    && curl -L http://172.17.0.1:20000/CMake.zip -o CMake.zip \
#    && unzip -d Tools/ -q CMake.zip; \
#    rm -rf CMake.zip\
#    rm -rf WIX.zip \
#    && echo "Fix Permissions" \
#    && chmod +x /usr/local/bin/wine64-entrypoint \
#    && chown -R wine:root ./Tools Tools/VS2019 Tools/SDK || echo "Done" \
#    && chmod -R 775 ./Tools Tools/VS2019 Tools/SDK || echo "Done" ;
#

USER wine
