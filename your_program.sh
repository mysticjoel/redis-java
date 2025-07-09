
set -e # Exit early if any commands fail

(
  cd "$(dirname "$0")" # Ensure compile steps are run within the repository directory1
  mvn -q -B package -Ddir=/tmp/codecrafters-build-redis-java
)

# Copied from .codecrafters/run.sh
#
# - Edit this to change how your program runs locally
# - Edit .codecrafters/run.sh to change how your program runs remotely
exec java -jar /tmp/codecrafters-build-redis-java/codecrafters-redis.jar "$@"
