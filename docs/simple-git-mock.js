// Mock for @napi-rs/simple-git to skip slow git timestamp lookups in development
// The real library calls getFileLatestModifiedDateAsync which is slow in large repos (10k+ commits)

export class Repository {
  static discover() {
    // Return null to skip git timestamp lookups entirely
    // This makes Nextra fall back to not showing "Last updated" timestamps
    throw new Error('Git repository mocked for development performance')
  }

  path() {
    return ''
  }

  isShallow() {
    return false
  }

  async getFileLatestModifiedDateAsync() {
    return new Date()
  }
}
