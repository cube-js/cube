function setUTM(link: string, source: string, campaign?: string): string {
    const url = new URL(link);

    url.searchParams.set('utm_medium', 'purple');
    url.searchParams.set('utm_source', source);
    url.searchParams.set('utm_campaign', campaign || Date.now().toString());

    return url.toString();
}

export { setUTM }
