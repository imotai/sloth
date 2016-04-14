GIT_BRANCH=`git branch | grep "*" | sed 's/\*//'`
LOG_VERSION=`git log -1 | grep commit | awk '{print $2}'`
AUTHOR=`git config user.email`
GIT_URL=`git remote -v | grep push | head -n 1 |awk '{print $2}'`
BUILD_DATE=`date`
BUILD_HOSTNAME=`hostname`
GCC_VERSION=`gcc --version | head -n 1`
KERNEL=`uname -r | head -n 1`
build_header () {
  echo "#include <iostream>"
  echo "#include <string>"
  echo ""
}

build_var () {

  echo ""
  echo "static const std::string GIT_BRANCH=\"$GIT_BRANCH\"; "
  echo "static const std::string LOG_VERSION=\"$LOG_VERSION\"; "
  echo "static const std::string AUTHOR=\"$AUTHOR\"; "
  echo "static const std::string GIT_URL=\"$GIT_URL\"; "
  echo "static const std::string BUILD_DATE=\"$BUILD_DATE\"; "
  echo "static const std::string BUILD_HOSTNAME=\"$BUILD_HOSTNAME\"; "
  echo "static const std::string GCC_VERSION=\"$GCC_VERSION\"; "
  echo "static const std::string KERNEL=\"$KERNEL\"; "

  echo ""
}

build_print_func () {
  echo "void PrintVersion() {"
  echo "  std::cout << \"Author:\" << AUTHOR << std::endl;"
  echo "  std::cout << \"Git:\" << GIT_URL << std::endl;"
  echo "  std::cout << \"Branch:\" << GIT_BRANCH << std::endl;"
  echo "  std::cout << \"Date:\" << BUILD_DATE << std::endl;"
  echo "  std::cout << \"Version:\" << LOG_VERSION << std::endl;"
  echo "  std::cout << \"Host:\" << BUILD_HOSTNAME << std::endl;"
  echo "  std::cout << \"Gcc:\" << GCC_VERSION << std::endl;"
  echo "  std::cout << \"Kernel:\" << KERNEL<< std::endl;"
  echo "}"
}
test -e src/version.h && rm src/version.h
build_header > src/version.h
build_var >> src/version.h
build_print_func >> src/version.h

