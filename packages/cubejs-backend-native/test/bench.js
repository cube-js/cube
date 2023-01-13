const native = require('../dist/js/index');

let called = 0;

async function callback() {
    called += 1;

    return {
        called
    };
}

(async () => {
    await native.bench_js_call({
        callback
    });

    console.log('Results', {
        called
    });
})();
