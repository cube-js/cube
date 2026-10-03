import type { UnsubscribeObj } from '@cubejs-client/core';

export interface ActiveRequest {
  generation: number;
  abortController: AbortController;
}
type InternalActiveRequest = ActiveRequest & {
  subscription: UnsubscribeObj | null;
};

async function unsubscribeSafely(value: UnsubscribeObj | null): Promise<void> {
  if (!value) {
    return;
  }

  try {
    await value.unsubscribe();
  } catch {
    // Cleanup failures must not replace the request result/error state.
  }
}

export class RequestLifecycle {
  private generation = 0;

  private active: InternalActiveRequest | null = null;

  private destroyed = false;

  public begin(): ActiveRequest {
    if (this.destroyed) {
      throw new Error('Cannot start a request after its lifecycle was destroyed.');
    }

    const previous = this.active;
    const active: InternalActiveRequest = {
      generation: ++this.generation,
      abortController: new AbortController(),
      subscription: null,
    };
    this.active = active;

    if (previous) {
      previous.abortController.abort();
      void unsubscribeSafely(previous.subscription);
    }

    return active;
  }

  public isCurrent(generation: number): boolean {
    return (
      !this.destroyed &&
      this.active?.generation === generation &&
      !this.active.abortController.signal.aborted
    );
  }

  public setSubscription(
    generation: number,
    subscription: UnsubscribeObj
  ): void {
    if (!this.isCurrent(generation) || !this.active) {
      void unsubscribeSafely(subscription);
      return;
    }

    this.active.subscription = subscription;
  }

  public async cancel(): Promise<void> {
    const active = this.active;
    this.active = null;
    this.generation += 1;

    if (!active) {
      return;
    }

    active.abortController.abort();
    await unsubscribeSafely(active.subscription);
  }

  public async destroy(): Promise<void> {
    if (this.destroyed) {
      return;
    }

    await this.cancel();
    this.destroyed = true;
  }
}
