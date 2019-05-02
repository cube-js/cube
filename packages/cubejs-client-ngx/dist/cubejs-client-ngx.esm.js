import { Injectable, Inject, NgModule } from '@angular/core';
import { from, Observable } from 'rxjs';
import cubejs from '@cubejs-client/core';

/*! *****************************************************************************
Copyright (c) Microsoft Corporation. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at http://www.apache.org/licenses/LICENSE-2.0

THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
MERCHANTABLITY OR NON-INFRINGEMENT.

See the Apache Version 2.0 License for specific language governing permissions
and limitations under the License.
***************************************************************************** */

function __decorate(decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
}

function __param(paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
}

function __metadata(metadataKey, metadataValue) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(metadataKey, metadataValue);
}

function __awaiter(thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
}

let CubejsClient = class CubejsClient {
    constructor(config) {
        this.config = config;
    }
    apiInstace() {
        if (!this.cubeJsApi) {
            this.cubeJsApi = cubejs(this.config.token, this.config.options);
        }
        return this.cubeJsApi;
    }
    load(...params) {
        return from(this.apiInstace().load(...params));
    }
    sql(...params) {
        return from(this.apiInstace().sql(...params));
    }
    meta(...params) {
        return from(this.apiInstace().meta(...params));
    }
    watch(query, params = {}) {
        return Observable.create(observer => query.subscribe({
            next: (query) => __awaiter(this, void 0, void 0, function* () {
                const resultSet = yield this.apiInstace().load(query, params);
                observer.next(resultSet);
            })
        }));
    }
};
CubejsClient = __decorate([
    Injectable(),
    __param(0, Inject('config')),
    __metadata("design:paramtypes", [Object])
], CubejsClient);

var CubejsClientModule_1;
let CubejsClientModule = CubejsClientModule_1 = class CubejsClientModule {
    static forRoot(config) {
        return {
            ngModule: CubejsClientModule_1,
            providers: [
                CubejsClient,
                {
                    provide: 'config',
                    useValue: config
                }
            ]
        };
    }
};
CubejsClientModule = CubejsClientModule_1 = __decorate([
    NgModule({
        providers: [CubejsClient]
    })
], CubejsClientModule);

/*
 * Public API Surface of cubejs-client-ngx
 */

// This file is not used to build this module. It is only used during editing

export { CubejsClientModule, CubejsClient };
