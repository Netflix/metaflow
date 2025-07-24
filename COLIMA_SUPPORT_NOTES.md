# Colima Support for metaflow-dev

## Issue Analysis
- metaflow-dev currently only detects Docker Desktop on macOS
- Need to add support for Colima as an alternative Docker runtime
- Colima uses a different socket location: ~/.colima/default/docker.sock

## Code Locations to Check
1. Docker detection logic
2. Socket path configuration  
3. Error messages mentioning Docker Desktop

## Implementation Plan
1. Find the current Docker detection code
2. Add Colima detection alongside Docker Desktop
3. Configure DOCKER_HOST when Colima is detected
4. Update error messages to mention both options
5. Add tests for both scenarios

## Testing Checklist
- [ ] Test with Colima running
- [ ] Test with Docker Desktop running
- [ ] Test with neither running
- [ ] Test switching between them
