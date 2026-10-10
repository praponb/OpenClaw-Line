/**
 * Tools never throw: failures come back as text so the agent can report
 * "Insufficient evidence" instead of the run aborting.
 */
export function toolError(message: string): string {
  return `Tool error: ${message}`;
}
