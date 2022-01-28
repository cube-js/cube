import { JWTOptions, RequestContext } from "./interfaces";

export class SecurityContextExtractor {
  private checkAuthDeprecationShown = false;

  public constructor(
    protected readonly logger: any,
    protected readonly options?: JWTOptions,
  ) {
  }

  public extract(ctx: Readonly<RequestContext>): any {
    if (this.options?.claimsNamespace) {
      if (typeof ctx.securityContext === 'object' && ctx.securityContext !== null) {
        if (<string> this.options.claimsNamespace in ctx.securityContext) {
          return ctx.securityContext[<string> this.options.claimsNamespace];
        }
      }

      return {};
    }

    let securityContext: any = {};

    if (typeof ctx.securityContext === 'object' && ctx.securityContext !== null) {
      if (ctx.securityContext.u) {
        if (!this.checkAuthDeprecationShown) {
          this.logger('JWT U Property Deprecation', {
            warning: (
              'Storing security context in the u property within the payload is now deprecated, please migrate: ' +
              'https://github.com/cube-js/cube.js/blob/master/DEPRECATION.md#authinfo'
            )
          });

          this.checkAuthDeprecationShown = true;
        }

        securityContext = {
          ...ctx.securityContext,
          ...ctx.securityContext.u,
        };

        delete securityContext.u;
      } else {
        securityContext = ctx.securityContext;
      }
    }

    return securityContext;
  }
}
