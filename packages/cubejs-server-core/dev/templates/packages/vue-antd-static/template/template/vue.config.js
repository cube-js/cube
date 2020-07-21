const path = require("path");
function resolve(dir) {
    return path.join(__dirname, dir);
}

module.exports = {
    lintOnSave:false,
    devServer: {
        overlay:{
            warning:false,
            errors:false
        },
    },
    chainWebpack: config => {
        config.resolve.alias
            .set("~", resolve("./src"))
    },
}
