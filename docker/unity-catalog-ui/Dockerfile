FROM unitycatalog/unitycatalog-ui:main

WORKDIR /ui

RUN sed -i 's#"proxy": "http://server:8080"#"proxy": "http://unity-catalog:8080"#' package.json
