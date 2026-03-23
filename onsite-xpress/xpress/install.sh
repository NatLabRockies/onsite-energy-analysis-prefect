#!/bin/bash

# This script installs the FICO Xpress Optimizer and sets up the environment.
# Run on a unix system, from an extracted xp*_setup.tar folder, with an (at least one)
# xp*.tar.gz archive present in that folder as well (default).
#
# Usage:
# ./install.sh
# ./install.sh --help

# Output to stderr. Log to errorlog. Usage:
# echoerr "message"
echoerr() {
  echo "$1" 1>&2;
  echo "$1" >> ${ERRORLOG}
}

# Output to stdout, log to installlog. Usage:
# echoerr "message" ["silent"]
echoinstall() {
  if [ ! "$2" = "silent" ]; then
    echo "$1"
  fi
  echo "$1" >> ${INSTALLLOG}
}

# Output var to stdout, log to installlog. Usage:
# echoerr VARNAME ["silent"]
echoinstall_var() {
  echoinstall "$1 is ${!1}" $2
}

# Output error and exit. Usage:
# failure "message"
failure() {
  echoerr "Error: $1"
  exit 1
}

# Set DISPLAY_CMD to a pager tool, or default to cat. Usage:
# set_display_command
set_display_command() {
  if [ -n "$PAGER" ]; then
    DISPLAY_CMD=$PAGER
  else
    DISPLAY_CMD=cat
    for i in pager less more; do
      if $i --help >/dev/null 2>&1; then
        DISPLAY_CMD=$i
        break
      fi
    done
  fi
}

# Display a license with a pager installed on the user's system.
# First print a message and wait for input, then display the actual license.
# The question for acceptance needs to be handled separately. Usage
# display_license "message" license.txt
display_license() {
  set_display_command
  read -n 1 -p "$1. Press any key to continue."
  echo ""
  if ! $DISPLAY_CMD $2; then
    failure "There was an error displaying the license. Exiting"
  fi
}

# Prompts the user a question to set a parameter. Usage:
# COMPONENT=n # set default
# get_parameter COMPONENT "question"
get_parameter() {
  variable=$1
  prompt=$2

  eval default=${!variable}

  printf "$prompt "
  if [ ! -z "$default" ]; then
    printf "[$default] "
  fi
  read reply
  if [ ! -z "$reply" ]; then
    eval $variable=$reply
  else
    eval $variable=$default
  fi
}

# In interactive mode, rompt the user a message, exit if answer is contains n or N. Usage:
# check_continue "message"
check_continue() {
  if [ "$INTERACTIVE_MODE" = "no" ]; then
    return
  fi
  prompt=$1
  if [ ! -z "$prompt" ]; then
    echo $prompt
  fi
  reply=n
  get_parameter reply "Do you want to continue the installation? [y]es, [n]o:"
  if echo $reply | grep -i n > /dev/null; then
    exit 1
  fi
}

# Sets variable and disables user prompt.
# Throws an error if expected values are required and INPUTVAR doesn't match.
# Only call from inital loop reading user input.
# Usage:
# optarg_read_var VARNAME INPUTVAR argumentname [expectedvalues]
optarg_read_var() {
  if [ ! -z "$2" ]; then
    eval "$1=$2"
    if [ ! -z "$4" ] && [ "$(echo "$2" | egrep -c "$4")" -ne 1 ]; then
      failure "Invalid value specified for argument $3."
    fi
  else
    failure "$3 requires a non-empty argument."
  fi
}

# Sets variable to yes or no and disables user prompt.
# Throws an error if INPUTVAR holds something different to 'yes' or 'no'.
# Only call from inital loop reading user input.
# Usage:
# optarg_var_enable VARNAME
optarg_var_enable() {
  eval "$1=yes"
}

# set component_name name for component variable. Sets
# COMPONENT_NAME to component_name_string
# Usage:
# get_component_name COMPONENT
get_component_name() {
  case $1 in
    MOSEL) COMPONENT_NAME="FICO Xpress Mosel"
      ;;
    KALIS) COMPONENT_NAME="FICO Xpress Kalis constraint programming engine"
      ;;
    CLI) COMPONENT_NAME="FICO Xpress commandline interface"
      ;;
    INTERFACES) COMPONENT_NAME="FICO Xpress Optimizer interfaces"
      ;;
    HEADERS) COMPONENT_NAME="FICO Xpress development files"
      ;;
    EXAMPLES) COMPONENT_NAME="Examples"
      ;;
    XPNLL) COMPONENT_NAME="New Licensing Library"
      ;;
    INSIGHT_CLI) COMPONENT_NAME="FICO Xpress Insight commandline interface"
      ;;
  esac
}

# Prompts the user or reads from commandline arguments.
# displays information about component installation
# Sets
# <COMPONENT>_INSTALL : yes or no
# Usage:
# choose_component COMPONENT
choose_component() {
  get_component_name $1
  if [ "$INTERACTIVE_MODE" = "yes" ]; then
    if [ "$1" = "KALIS" ] || [ "$1" = "INSIGHT_CLI" ]; then
      R=no
    else
      R=yes
    fi
    while [[ ! "$(eval echo "$(echo \$OPT_${1}_INSTALL)")" =~ yes|no ]]; do
      get_parameter R "Do you wish to install the $COMPONENT_NAME components? [y]es, [n]o:"
      if echo $R | grep -i y > /dev/null; then
        eval OPT_$1_INSTALL=yes
      elif echo $R | grep -i n > /dev/null; then
        eval OPT_$1_INSTALL=no
      elif echo $R | grep -i q > /dev/null; then
        echo "Exiting"
        exit 1
      else
        echo "Could not understand response, please enter Y or N or Q (quit)"
      fi
    done
  fi

  if [ "$OPT_KALIS_LICENSE" = "yes" ]; then
    OPT_KALIS_INSTALL=yes
  fi

  if [ "$1" = "KALIS" ] && [ "$OPT_KALIS_INSTALL" = "yes" ]; then
    R=no
    if [ "$INTERACTIVE_MODE" = "yes" ]; then
      if [[ ! "$(eval echo "$(echo \$OPT_${1}_LICENSE)")" =~ yes|no ]]; then
        # Display license and check for acceptance
        display_license "You will be prompted with the Kalis license file, you can also find it in the file 'kalis_license.txt'" kalis_license.txt
        get_parameter R "Do you accept the license terms of the Kalis license agreement? [y]es, [n]o:"
        if [ "$R" = "yes" ] || [ "$R" = "y" ]; then
          eval OPT_KALIS_LICENSE=yes
        else
          eval OPT_KALIS_LICENSE=no
        fi
      fi
    fi
  fi

  if [ "$(eval echo "$(echo \$OPT_${1}_INSTALL)")" = "yes" ]; then
    if [ "$1" = "KALIS" ] && [ ! "$OPT_KALIS_LICENSE" = "yes" ]; then # license agreement
      eval OPT_${1}_INSTALL=no
      echoinstall "$COMPONENT_NAME components will NOT be installed, missing agreement to license terms."
    else
      eval ${1}_INSTALL=yes
      echoinstall "$COMPONENT_NAME components will be installed."
    fi
  else
    eval ${1}_INSTALL=no
    echoinstall "$COMPONENT_NAME components will NOT be installed."
  fi

  # Add component choice to log:
  echoinstall_var "${1}_INSTALL" "silent"
}

# Chooses all the components for installation. Sets
# - KALIS_INSTALL
# - MOSEL_INSTALL
# - CLI_INSTALL
# - INTERFACES_INSTALL
# - HEADERS_INSTALL
# - EXAMPLES_INSTALL
# - XPNLL_INSTALL
# - INSIGHT_CLI_INSTALL
# Usage:
# choose_components
choose_components() {
  if [ "$PACKAGE_TYPE" = "distrib_server" ]; then
    return
  fi
  if [ "$OPT_COMPONENTS" = "full" ]; then
    echoinstall "Installation type: full"
  fi
  echoinstall

  for component in $(echo ${OPT_COMPONENTS} | tr ',' ' '); do
    case $component in
      full)
        OPT_KALIS_INSTALL=yes
        OPT_MOSEL_INSTALL=yes
        OPT_CLI_INSTALL=yes
        OPT_INTERFACES_INSTALL=yes
        OPT_HEADERS_INSTALL=yes
        OPT_EXAMPLES_INSTALL=yes
        OPT_XPNLL_INSTALL=yes
        OPT_INSIGHT_CLI_INSTALL=yes
        break
        ;;
      kalis)
        OPT_KALIS_INSTALL=yes
        ;;
      mosel)
        OPT_MOSEL_INSTALL=yes
        ;;
      cli)
        OPT_CLI_INSTALL=yes
        ;;
      interfaces)
        OPT_INTERFACES_INSTALL=yes
        ;;
      dev-components)
        OPT_HEADERS_INSTALL=yes
        ;;
      examples)
        OPT_EXAMPLES_INSTALL=yes
        ;;
      xpnll)
        OPT_XPNLL_INSTALL=yes
        ;;
      insight-cli)
        OPT_INSIGHT_CLI_INSTALL=yes
        ;;
      *)
        echo "Error: could not parse command-line parameter '$component' in 'components'."
        print_usage
        exit 1
        ;;
    esac
  done

  choose_component KALIS
  choose_component MOSEL
  choose_component CLI
  choose_component INTERFACES
  choose_component HEADERS
  choose_component EXAMPLES
  choose_component XPNLL
  choose_component INSIGHT_CLI
  echoinstall
}

# Chooses location, license, components and if supposed to be included into bashrc. Sets
# - PKGFILE           : the name of the tarball
# - LICENSE_METHOD    : license method
# - PACKAGE_TYPE      : xpress_package, distrib_server, distrib_client
# - XPRESSDIR         : location for xpress installation
# - COPY_LICENSE      : yes, no
# - NEED_LIC          : distrib, pc
# - LICPATH           : user specified license path or community license location
# - INCLUDE_IN_BASHRC : yes, no
# Usage:
# choose_setup
choose_setup() {
  choose_installation
  accept_xpress_license
  choose_license_and_package_type
  choose_location
  set_license_path
  choose_components
  choose_include_in_bashrc
}

# Prompts the user for the license and aborts if it wasn't accepted
# Usage:
# accept_xpress_license
accept_xpress_license() {
  license_file=license.txt
  if [ ! -f "$license_file" ]; then
    tar -x -o -z -f $PKGFILE $license_file
  fi

  if [ -z "$OPT_XPRESS_LICENSE" ] && [ "$INTERACTIVE_MODE" = "yes" ]; then
    display_license "You will be prompted the Xpress license" $license_file
    OPT_XPRESS_LICENSE=no
    get_parameter OPT_XPRESS_LICENSE "Do you accept this license? [y]es, [n]o:"
  fi

  if [ "$OPT_XPRESS_LICENSE" = "yes" ] || [ "$OPT_XPRESS_LICENSE" = "y" ]; then
    return
  else
    failure "Xpress license was not accepted. Exiting installer."
  fi
}

# Prompt the user to choose an installation package and a licensing model.
# choose_license_method needs to be called before. Sets
# PKGFILE           : the name of the tarball
# Usage:
# choose_installation
choose_installation() {
  platforms=$(ls ${WORKINGDIR}/x*.tar.gz)

  numbertgz=0;
  for g in $platforms; do
    numbertgz=$((numbertgz + 1))
  done

  i=0
  # for all available tarballs list the platforms
  for f in $platforms; do
    i=$((i + 1))

    eval pkg_$i=$f
    p=$(basename $f .tar.gz | cut -f 2- -d _)

    if [ $p = linux_x86 ]; then
      plat="Linux (GLIBC 2.2) on x86"
    elif [ $p = linuxrh9_x86 ]; then
      plat="Linux (GLIBC 2.3+) on x86"
    elif [ $p = linux_ia64 ]; then
      plat="Linux (GLIBC 2.3+) on Itanium 2 (64-bit)"
    elif [ $p = linux_x86_64 ]; then
      plat="Linux (GLIBC 2.3+) on AMD64 / EM64T"
    elif [ $p = linux_openmp_x86_64 ]; then
      plat="Linux (GLIBC 2.5+ with OpenMP) on AMD64 / EM64T"
    elif [ $p = sun_sparc ]; then
      plat="Sun Solaris on SPARC"
    elif [ $p = sun_sparc64 ]; then
      plat="Sun Solaris on 64-bit SPARC"
    elif [ $p = sun_x86_64 ]; then
      plat="Sun Solaris on AMD64 / EM64T"
    elif [ $p = aix_ppc ]; then
      plat="IBM AIX on PowerPC"
    elif [ $p = aix43_rs64 ]; then
      plat="IBM AIX 4.3 on 64-bit PowerPC"
    elif [ $p = aix51_rs64 ]; then
      plat="IBM AIX 5 on 64-bit PowerPC"
    elif [ $p = hpux_pa ]; then
      plat="HP-UX on PA-RISC"
    elif [ $p = hpux_pa64 ]; then
      plat="HP-UX on 64-bit PA-RISC"
    elif [ $p = hpux_ia64 ]; then
      plat="HP-UX on Itanium 2 (64-bit)"
    elif [ $p = macos_x86_64 ]; then
      plat="MacOS on AMD64 / EM64T"
    elif [ $p = macos_arm64 ]; then
      plat="MacOS on ARM64"
    else
      plat="Unknown platform ($p)"
    fi

    if [ $numbertgz -gt 1 ]; then
      echo "$i) $f for $plat"
    fi
  done

  if [ $numbertgz -gt 1 ]; then
    echo
  fi

  if [ $i = 0 ]; then
    echo "Unable to find any FICO Xpress packages in the ${WORKINGDIR} directory."
    echo "Please ensure that this installer is run from the directory in which"
    echo "you extracted the FICO Xpress tar file."
    exit 1
  elif [ $i = 1 ]; then
    PKGFILE=$pkg_1
  else
    if [ "$INTERACTIVE_MODE" = "no" ]; then
      failure "Cannot determine which package to install. Please make sure there is just one installer present or run installer in interactive mode."
    fi
    # have the user choose one of the available packages
    while [ -z "$PKGFILE" ]; do
      get_parameter pnum "Which package do you wish to install? (1 - $i)"

      if [ "$pnum" -lt 1 ] || [ "$pnum" -gt $i ]; then
        check_continue "Invalid option chosen."
        pnum=
      else
        PKGFILE=$(eval echo \$\{pkg_$pnum\})
      fi
    done
  fi

  # Add package name entered to log:
  echoinstall "Installing package '$PKGFILE'"
  echoinstall

  # Add distributed install type to log:
  echoinstall_var PKGFILE "silent"
}

# Selects license method and package type. Sets
# - LICENSE_METHOD    : license method
# - PACKAGE_TYPE      : xpress_package, distrib_server, distrib_client
# Usage:
# choose_license_and_package_type
choose_license_and_package_type() {
  # Check whether we want this to be a static or distributed install:

  if [ -z "$OPT_LICENSE_TYPE" ]; then
    if [ "$INTERACTIVE_MODE" = "no" ]; then
      failure "Cannot determine license method. Please call the installer with a license type specified."
    fi
    OPT_LICENSE_TYPE=
    while [ -z "$OPT_LICENSE_TYPE" ]; do
      reply=
      echo "License Types"
      echo "[c] Community Licensing (use Xpress for free; some usage restrictions apply)"
      echo "[s] Static Licensing ('Node-locked' or 'USB dongle')"
      echo "[f] Floating licensing (separate license server)"
      echo "[w] Web floating licensing (online license server)"
      get_parameter reply "Please select license type or [q]uit the installer:"

      if echo $reply | grep -i c > /dev/null; then
        OPT_LICENSE_TYPE=community
      elif echo $reply | grep -i w > /dev/null; then
        OPT_LICENSE_TYPE=web-floating
      elif echo $reply | grep -i s > /dev/null; then
        OPT_LICENSE_TYPE=static
      elif echo $reply | grep -i f > /dev/null; then
        OPT_LICENSE_TYPE=distributed
      elif echo $reply | grep -i q > /dev/null; then
        exit 1
      else
        echo "Could not understand response, please try again."
        echo
      fi
    done
    if [ "$OPT_LICENSE_TYPE" = "distributed" ]; then
      PACKAGE_TYPE=
      while [ -z "$PACKAGE_TYPE" ]; do
        echo ""
        reply=
        get_parameter reply "Is this a [s]erver or a [c]lient installation? ([q]uit installer)"

        if echo $reply | grep -i c > /dev/null; then
          PACKAGE_TYPE=distrib_client
        elif echo $reply | grep -i s > /dev/null; then
          PACKAGE_TYPE=distrib_server
        elif echo $reply | grep -i q > /dev/null; then
          exit 1
        else
          echo "Could not understand response, please try again:"
          echo
        fi
      done
    fi
  fi

  case $OPT_LICENSE_TYPE in
    community)
      LICENSE_METHOD=community
      PACKAGE_TYPE=xpress_package
      ;;
    static)
      LICENSE_METHOD=static
      PACKAGE_TYPE=xpress_package
      ;;
    web-floating)
      LICENSE_METHOD=static
      PACKAGE_TYPE=xpress_package
      ;;
    floating-server)
      LICENSE_METHOD=distributed
      PACKAGE_TYPE=distrib_server
      ;;
    floating-client)
      LICENSE_METHOD=distributed
      PACKAGE_TYPE=distrib_client
      ;;
  esac

  # Add entered license method to log:
  echoinstall_var LICENSE_METHOD "silent"
  echoinstall_var PACKAGE_TYPE "silent"
}

# Ask user to choose a location for the installation.
# choose_installation needs to be called before this.
# Also checks if the directory is writable for installation. Sets
# - XPRESSDIR         : location for xpress installation
# Usage:
# choose_location
choose_location() {
  if [ -z "$OPT_INSTALL_PATH" ]; then
    XPRESSDIR=/opt/xpressmp
    if [ "$INTERACTIVE_MODE" = "yes" ]; then
      if [ "$PACKAGE_TYPE" = "distrib_client" ]; then
        get_parameter XPRESSDIR "Where do you want to install the client?"
      elif [ "$PACKAGE_TYPE" = "xpress_package" ]; then
        get_parameter XPRESSDIR "Where do you want to install the Xpress-MP?"
      elif [ "$PACKAGE_TYPE" = "distrib_server" ]; then
        get_parameter XPRESSDIR "Where do you want to install the license server?"
      fi
    fi
  else
    XPRESSDIR=$OPT_INSTALL_PATH
  fi

  echoinstall "Installing into $XPRESSDIR."
  check_permissions
}

# If not a distributed client installation, checks if license file exists and sets
# - COPY_LICENSE      : yes, no
# - NEED_LIC          : distrib, pc
# - LICPATH           : user specified license path or community license location
# Usage:
# set_license_path
set_license_path() {
  if [ "$LICENSE_METHOD" = "community" ]; then
    COPY_LICENSE=yes
    LICPATH=$XPRESSDIR/bin/community-xpauth.xpr
    return
  fi
  if [ "$PACKAGE_TYPE" = "distrib_client" ]; then
    return
  fi

  if [ ! -z "$OPT_XPAUTH_PATH" ] && [ ! -f "$OPT_XPAUTH_PATH/xpauth.xpr" ]; then
    # if user has given a wrong path to license, pretend he hasn't
    echoinstall "Could not find license file $OPT_XPAUTH_PATH/xpauth.xpr in specified location."
    OPT_XPAUTH_PATH=
  fi

  if [ ! -z "$OPT_XPAUTH_PATH" ]; then
    LICPATH=$OPT_XPAUTH_PATH/xpauth.xpr
  else
    if [ "$INTERACTIVE_MODE" = "no" ]; then
      failure "Missing path to valid license file."
    fi

    # ask user if they have a license
    while [ -z "$(echo $received_license | sed -n /^[YyNn]/p)" ]; do
      echo ""
      get_parameter received_license "Have you received a classic license or floating license configuration file xpauth.xpr from Xpress Support? [y]es, [n]o:"
    done
    if echo $received_license | grep -i y > /dev/null; then
      # user does have a license, now find the correct location
      while [ -z "$OPT_XPAUTH_PATH" ]; do
        get_parameter OPT_XPAUTH_PATH "Please enter the directory containing your xpauth.xpr file:"
        if [ -d "$OPT_XPAUTH_PATH" ]; then
          OPT_XPAUTH_PATH=$OPT_XPAUTH_PATH/xpauth.xpr
        fi
        if [ ! -f "$OPT_XPAUTH_PATH" ]; then
          check_continue "Could not find license file $OPT_XPAUTH_PATH"
          OPT_XPAUTH_PATH=
        fi
      done
      LICPATH=$OPT_XPAUTH_PATH
    else
      # user doesn't have a license, help them apply for one
      if [ "${OPT_LICENSE_TYPE}" = "web-floating" ]; then
        echoinstall "To order a license file, please email support@fico.com."
      else
        if [ "$PACKAGE_TYPE" = "distrib_server" ]; then
          NEED_LIC=distrib
          ${WORKINGDIR}/utils/xphostid 2>>${ERRORLOG} >hostid.log
        else
          NEED_LIC=pc
          ${WORKINGDIR}/utils/xphostid 2>>${ERRORLOG} >hostid.log
        fi
        if [ -s hostid.log ]; then
          echoinstall "To order a license file, please email support@fico.com, quoting the following:"
          cat hostid.log
        else
          echoinstall "To order a license file, please follow the instructions at https://www.fico.com/en/fico-xpress-trial-and-licensing-options."
        fi
      fi
      echoinstall "You can proceed with the installation and install the license key later. For instructions, refer to https://www.fico.com/fico-xpress-optimization/docs/latest/installguide/dhtml/chapinst2.html."
      check_continue
      LICPATH=
    fi
  fi

  if [ ! -z "$LICPATH" ]; then
    if [ "$INTERACTIVE_MODE" = "yes" ]; then
      reply=yes
      get_parameter reply "Do you want to copy your license file into $XPRESSDIR/bin? [y]es, [n]o:"

      if echo $reply | grep -i y > /dev/null; then
        COPY_LICENSE=yes
      elif echo $reply | grep -i n > /dev/null; then
        COPY_LICENSE=no
      fi
    else
      COPY_LICENSE=yes
    fi
  fi

  echoinstall_var LICPATH "silent"
  echoinstall_var NEED_LIC "silent"
  echoinstall_var COPY_LICENSE "silent"
}

# Depending on OPT_ parameters, sets
# - INCLUDE_IN_BASHRC : yes, no
# Usage:
# choose_include_in_bashrc
choose_include_in_bashrc() {
  if [ "$PACKAGE_TYPE" = "distrib_server" ]; then
    INCLUDE_IN_BASHRC=no
    return
  fi

  if [ "$INTERACTIVE_MODE" = "yes" ]; then
    while [ -z "$OPT_INCLUDE_IN_BASHRC" ]; do
      reply=no
      get_parameter reply "Do you want to add Xpress installation paths to .bashrc file? [y]es, [n]o, [q]uit:"
      if echo $reply | grep -i y > /dev/null; then
        OPT_INCLUDE_IN_BASHRC=yes
      elif echo $reply | grep -i n > /dev/null; then
        OPT_INCLUDE_IN_BASHRC=no
      elif echo $reply | grep -i q > /dev/null; then
        echo "Exiting."
        exit
      else
        echo "Could not understand response, enter Y, N or Q."
      fi
    done
  fi

  if [ "$OPT_INCLUDE_IN_BASHRC" = "yes" ]; then
    INCLUDE_IN_BASHRC=yes
    echoinstall "Including Xpress installation paths to .bashrc."
  else
    INCLUDE_IN_BASHRC=no
    echoinstall "Not including Xpress installation paths to .bashrc."
  fi
  echoinstall ""
}

# For XPRESSDIR, check if the directory can be created or if the user has write-access.
# Throws an error if that is not the case.
# Usage:
# check_permissions
check_permissions() {
  if [ ! -d "$XPRESSDIR" ]; then
    if [ "${XRPESSDIR::5}" = "/opt/" ] && [ ! -d "/opt/" ]; then
      mkdir --mode=755 /opt || failure "Can't create directory /opt."
    fi
    mkdir -p $XPRESSDIR 2>>${ERRORLOG} || failure "Can't create $XPRESSDIR: permission denied"
  else
    # just check for write-access
    if [ ! -x $XPRESSDIR ] || [ ! -w $XPRESSDIR ]; then
      failure "Can't modify '$XPRESSDIR': permission denied"
    fi
  fi
}

# Extracts configured components from tarfile and sets up varscripts.
# Usage:
# setup_installation
setup_installation() {
  extract_tarfile
  remove_duplicate_bundled_libs
  setup_varscripts
}

# Unpack tarball making sure to move possible conflict files out of the way.
# Remove deprecated files and delete the ones not used/agreed upon.
# Usage:
# extract_tarfile
extract_tarfile() {
  echoinstall ""
  echoinstall "Extracting package $PKGFILE..."

  # move possible conflict files
  if [ "$PACKAGE_TYPE" = "distrib_client" ]; then
    if [ ! -d $XPRESSDIR/lib/backup ]; then
      mkdir $XPRESSDIR/lib/backup 2>/dev/null
    fi
    mv -f $XPRESSDIR/lib/libxprl* $XPRESSDIR/lib/backup 2>/dev/null
  fi

  # terminate full directories by a slash as that is how they appear in tar archive listing
  files_list="bin/community-xpauth.xpr
bin/runlmgr
bin/xphostid
bin/xplicquery
bin/xplicstat
bin/xpserver
bin/xpsync
docs/
lib/libxprl*
lib/libxknitro*
lib/libxprs*
lib/libxprsws*
lib/thirdparty/
license.txt
readme/
readme.html
relnotes.html
licenses/
version.txt"

  # New Licensing Library (optionnal dependancy)
  if [ "$XPNLL_INSTALL" = "yes" ]; then
    files_list="$files_list
lib/libxpnll*"
  fi

  # Interfaces components
  if [ "$INTERFACES_INSTALL" = "yes" ]; then
    files_list="$files_list
lib/libjavaxprs*
lib/xprb*.jar
lib/xprs*.jar
lib/libxprb_J*
lib/nuget/
bin/xprsdn.xml
matlab/
R/"
  fi

  # Examples components
  if [ "$EXAMPLES_INSTALL" = "yes" ]; then
    files_list="$files_list
examples/"
  fi

  # Kalis files
  if [ "$KALIS_INSTALL" = "yes" ]; then
    files_list="$files_list
bin/_kalis.so
docs/solver/kalis
kalis_license.txt
lib/artelys_fico_xpress_*.txt
lib/kalis.py
lib/libKalis.so
lib/ArtelysKalis.jar
lib/libKalisJava.so
licenses/kalis.license.txt"
  fi

  # Mosel components
  if [ "$MOSEL_INSTALL" = "yes" ]; then
    files_list="$files_list
bin/aec2setup
bin/mmssl
bin/xprnls
bin/a6electr.*
bin/mosel
bin/moseldoc
bin/xprmsrv
do/dso/do.dso
docs/mosel/
dso/
lib/libxprnls*
lib/aec2*.jar
lib/jsch-*.jar
lib/bindrv*.jar
lib/libbindrv*
lib/xprd*.jar
lib/libxprd*
lib/xprm*.jar
lib/libxprm*
lib/mosjvm.jar
matlab/moselexec.m
matlab/moselexec.mexa64"
  fi

  # commandline interface components
  if [ "$CLI_INSTALL" = "yes" ]; then
     files_list="$files_list
bin/optimizer"
  fi

  # Insight commandline interface components
  if [ "$INSIGHT_CLI_INSTALL" = "yes" ]; then
     files_list="$files_list
bin/xpwinsightcmd.sh
bin/insight-credentials.template
workbench/"
  fi

  # development headers
  if [ "$HEADERS_INSTALL" = "yes" ]; then
     files_list="$files_list
include/"
  fi

  # extract package and check if that command prints anything to standard error
  if [ "$PACKAGE_TYPE" = "distrib_server" ]; then
    if [ ! -d $XPRESSDIR/bin ]; then
      mkdir $XPRESSDIR/bin
    fi
    files_list="bin/runlmgr bin/xpserver bin/xplicstat bin/xphostid lib/thirdparty/libssl* lib/thirdparty/libcrypto*"
  fi

  files_list_pattern="^$(echo "$files_list" | tr '\n' ' ' | sed -e 's/ /$|^/g' -e 's/\*/.*/g' )"
  files_list_pattern=${files_list_pattern::$((${#files_list_pattern}-3))}
  pkglist=$(tar --list -f $PKGFILE)
  install_files=$(echo "$pkglist" | grep -Eo "(${files_list_pattern})" | sort -ru | tr '\n' ' ')
  echoinstall "Extracting the following files: $install_files"

  # Note: on modern platforms, tar can do the gzip part
  if [ "$( cd $XPRESSDIR; tar -x -o -z -f ${PKGFILE} $install_files 2>&1 | tee -a ${ERRORLOG})" ]; then
    failure "Errors while extracting the package - your download may be corrupted."
  fi

  # Now remove any old files that have moved to new locations
  rm -f $XPRESSDIR/do/dso/mosjvm.dso 2>/dev/null
  rm -f $XPRESSDIR/do/lib/mosjvm.jar 2>/dev/null

  echoinstall "Package extracted successfully."
  echoinstall ""
}

# If target system already contains libssh libssl or libcrypto ie in /usr/lib or /usr/lib64,
# then use that one instead of the bundled one. In that case, remove the libraries from the XPRESSDIR.
# Usage:
# remove_duplicate_bundled_libs
remove_duplicate_bundled_libs()
{
  if [[ "$(basename $PKGFILE .tar.gz | cut -f 2- -d _)" =~ macos ]]; then # don't run on macos
    return
  fi
  for libname in libssh libssl libcrypto; do
    lib_file_path=$(find "$XPRESSDIR/lib" -name "${libname}.so.*" | head -n1)
    if [ -z "${lib_file_path}" ]; then
      # Should never happen as Xpress always ships with these libraries
      echoinstall "Warning: ${libname} not found in Xpress installation."
    else
      lib_file_name=$(basename "${lib_file_path}")
      if [ $(ls /usr/lib*/${lib_file_name} 2>/dev/null | wc -l) -ne 0 ]; then
        echoinstall "Using system ${libname}"
        rm -f ${lib_file_path}
      fi
    fi
  done
}

# Ask user for the servername (if applies) and then adding license file to XPRESSDIR.
# This method only gets called if PACKAGE_TYPE is distributed client.
# Usage:
# generate_client_lic
generate_client_lic() {
  # Find out what the server is
  servername=
  if [ ! -z "$OPT_LICENSE_SERVER" ]; then
    servername=$OPT_LICENSE_SERVER
  elif [ "$INTERACTIVE_MODE" = "yes" ]; then
    get_parameter servername "Enter the name of your license server:"
  fi

  if [ -z "$servername" ]; then
    echoinstall "When you know the name of your server, edit $XPRESSDIR/bin/xpauth.xpr"
    echoinstall "and change server_name in \"use_server server=server_name\"."
    servername=server_name
  fi

  # Add server name entered to log:
  echoinstall "Using server_name '$servername'."

  # Check for prior existence of the server line:
  # Otherwise just write the file $XPRESSDIR/bin/xpauth.xpr, which is the license file that the distributed client uses
  if [ ! -z "$LICPATH" ] && grep -i use_server $LICPATH > /dev/null 2>&1; then
    echo "use_server server=\"$servername\"" > $XPRESSDIR/bin/xpauth.xpr
    sed $LICPATH -e /use_server/d >> $XPRESSDIR/bin/xpauth.xpr
  else
    echo "use_server server=\"$servername\"" > $XPRESSDIR/bin/xpauth.xpr
  fi

  # Copy the altered version back:
  copy_back=no # default, don't overwrite user's license
  if [ ! -z "$LICPATH" ] && [ "$INTERACTIVE_MODE" = "yes" ]; then
    get_parameter copy_back "Do you want to overwrite your original xpauth.xpr with the new client xpauth.xpr? [y]es, [n]o:"

    if echo $copy_back | grep -i y > /dev/null 2>&1; then
      echoinstall "Overwriting original xpauth.xpr with altered version from '$LICPATH'."
      cp -f $XPRESSDIR/bin/xpauth.xpr $LICPATH
    else
      echoinstall "Modified $XPRESSDIR/bin/xpauth.xpr, but leaving original '$LICPATH' unaltered."
    fi
  fi
}

# Customizes and installs varscripts based on the users preferences. Sets
# - XPAUTH_VAR  : path to license used for installation
# Usage:
# setup_varscripts
setup_varscripts() {
  customize_varscripts
  install_varscripts
}

# Set the XPAUTH_VAR, copy the license to the correct location and create the xpvars.{c,}sh script. Sets
# - XPAUTH_VAR  : path to license used for installation
# Usage:
# customize_varscripts
customize_varscripts() {
  if [ "$PACKAGE_TYPE" = "distrib_client" ]; then
    generate_client_lic
    XPAUTH_VAR=$XPRESSDIR/bin/xpauth.xpr
  elif [ ! -z "$LICPATH" ]; then
    if [ "$COPY_LICENSE" = "yes" ]; then
      # copy license file into new directory
      cp $LICPATH $XPRESSDIR/bin/xpauth.xpr
      chmod 644 $XPRESSDIR/bin/xpauth.xpr 2> /dev/null
      XPAUTH_VAR=$XPRESSDIR/bin/xpauth.xpr
    else
      XPAUTH_VAR=$LICPATH
    fi
  else
    # Set XPAUTH_VAR to expected default location for when user copies license there
    XPAUTH_VAR=$XPRESSDIR/bin/xpauth.xpr
  fi

  cat > $XPRESSDIR/bin/xpvars.sh <<EOF
XPRESSDIR=$XPRESSDIR
XPAUTH_PATH=$XPAUTH_VAR
LD_LIBRARY_PATH=\${XPRESSDIR}/lib:\${LD_LIBRARY_PATH}
DYLD_LIBRARY_PATH=\${XPRESSDIR}/lib:\${DYLD_LIBRARY_PATH}
SHLIB_PATH=\${XPRESSDIR}/lib:\${SHLIB_PATH}
LIBPATH=\${XPRESSDIR}/lib:\${LIBPATH}

CLASSPATH=\${XPRESSDIR}/lib/xprs.jar:\${CLASSPATH}
CLASSPATH=\${XPRESSDIR}/lib/xprb.jar:\${CLASSPATH}
CLASSPATH=\${XPRESSDIR}/lib/xprm.jar:\${CLASSPATH}
PATH=\${XPRESSDIR}/bin:\${PATH}

if [ -f "${XPRESSDIR}/bin/xpvars.local.sh" ]; then
  . ${XPRESSDIR}/bin/xpvars.local.sh
fi

export LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH
export SHLIB_PATH
export LIBPATH
export CLASSPATH
export XPRESSDIR
export XPAUTH_PATH
EOF

  cat > $XPRESSDIR/bin/xpvars.csh <<EOF
setenv XPRESSDIR $XPRESSDIR
setenv XPAUTH_PATH $XPAUTH_VAR

if ( \$?LD_LIBRARY_PATH ) then
  setenv LD_LIBRARY_PATH \${XPRESSDIR}/lib:\${LD_LIBRARY_PATH}
else
  setenv LD_LIBRARY_PATH \${XPRESSDIR}/lib
endif

if ( \$?DYLD_LIBRARY_PATH ) then
  setenv DYLD_LIBRARY_PATH \${XPRESSDIR}/lib:\${DYLD_LIBRARY_PATH}
else
  setenv DYLD_LIBRARY_PATH \${XPRESSDIR}/lib
endif

if ( \$?SHLIB_PATH ) then
  setenv SHLIB_PATH \${XPRESSDIR}/lib:\${SHLIB_PATH}
else
  setenv SHLIB_PATH \${XPRESSDIR}/lib
endif

if ( \$?LIBPATH ) then
  setenv LIBPATH \${XPRESSDIR}/lib:\${LIBPATH}
else
  setenv LIBPATH \${XPRESSDIR}/lib
endif

if ( \$?CLASSPATH ) then
  setenv CLASSPATH \${XPRESSDIR}/lib/xprs.jar:\${XPRESSDIR}/lib/xprm.jar:\${XPRESSDIR}/lib/xprb.jar:\${CLASSPATH}
else
  setenv CLASSPATH \${XPRESSDIR}/lib/xprs.jar:\${XPRESSDIR}/lib/xprm.jar:\${XPRESSDIR}/lib/xprb.jar
endif

set path=( \${XPRESSDIR}/bin \${path} )
EOF
  chmod 644 $XPRESSDIR/bin/xpvars.sh 2> /dev/null
  chmod 644 $XPRESSDIR/bin/xpvars.csh 2> /dev/null
}

# Adds the varscripts to the user profile if required.
# Usage:
# install_varscripts
install_varscripts() {
  # Now we've created the var scripts, add them to user profile if required
  if [ "$INCLUDE_IN_BASHRC" = "yes" ]; then
    if [ -e ~/.bashrc ]; then
      cp ~/.bashrc bashrc.bak
    fi
    cat >> ~/.bashrc <<EOF

# Use FICO Xpress
if [ -z "\$XPRESSDIR" -o ! -d "\$XPRESSDIR" ]; then
  . $XPRESSDIR/bin/xpvars.sh
fi

EOF
  fi
}

# Start license manager.
# Only gets called on a distributed server installation.
# Usage:
# start_lmgrd
start_lmgrd() {
  echoinstall "Starting the FICO Xpress license manager:"
  echo "$XPRESSDIR/bin/xpserver -d -xpress $XPAUTH_VAR -logfile /var/tmp/xpress.log"
  $XPRESSDIR/bin/xpserver -d -xpress $XPAUTH_VAR -logfile /var/tmp/xpress.log
}

# Generate an xprmsrv key.
# Doesn't get called on a distributed server installation.
# Sets and exports
# - LD_LIBRARY_PATH
# - DYLD_LIBRARY_PATH
# - SHLIB_PATH
# - LIBPATH
# Usage:
# generate_xprmsrv_key
generate_xprmsrv_key() {
  if [ "$MOSEL_INSTALL" = "yes" ]; then
    oldpwd=$(pwd)
    cd $XPRESSDIR/bin
    LD_LIBRARY_PATH=$XPRESSDIR/lib:$LD_LIBRARY_PATH
    DYLD_LIBRARY_PATH=$XPRESSDIR/lib:$DYLD_LIRBARY_PATH
    SHLIB_PATH=$XPRESSDIR/lib:$SHLIB_PATH
    LIBPATH=$XPRESSDIR/lib:$LIBPATH
    export LD_LIBRARY_PATH DYLD_LIBRARY_PATH SHLIB_PATH LIBPATH
    ./xprmsrv -key new
    cd "$oldpwd"
  fi
}


# Prints the help guide for this script.
# Usage:
# print_usage
print_usage() {
  echo "Usage: $SCRIPTNAME [-h] [-d install_path] [-a xpauth_file]
  [-l community|static|floating-server|floating-client] [-s server_name]
  [-p] [-o] [-c full,mosel,kalis,cli,interfaces,dev-components,examples]
  [-k]

  where:
  -h, --help                      show this help text

  -d, --install-path              path to target installation directory, default /opt/xpressmp/
  -l, --license-type              set the license type: community, static, web-floating, floating-server or floating-client
  -s, --license-server-name       license server name, if license type floating-client specified
  -a, --xpauth-path               path to license or license configuration file, or the directory containing xpauth.xpr

  -o, --no-interactive            turn off interactive mode.
  -p, --include-in-bashrc         add xpvars.sh in ~/.bashrc file

  -c, --components                'full' or comma separated list of the following components:
                                  - mosel           install the FICO Xpress Mosel components
                                  - kalis           install the FICO Xpress Kalis constraint programming engine
                                  - cli             install the FICO Xpress commandline interface
                                  - interfaces      install the FICO Xpress Optimizer interfaces
                                  - dev-components  install the FICO Xpress developer libraries and headers
                                  - examples        install the Examples
                                  - xpnll           install the Web Licensing Library
                                  - insight-cli     install the FICO Xpress Insight commandline interface
                                  defaults to minimal install

  -k, --accept-kalis-license      accept the Kalis license in 'kalis_license.txt'; this flag implicitly turns on the kalis component
  -x, --accept-xpress-license     accept the Xpress license in 'license.txt', to be found in the tarball to install
  "
  exit 0
}

###########################################
############# start of script #############
###########################################

SCRIPTNAME=$0
WORKINGDIR=$(dirname $(pwd -P)/$SCRIPTNAME)
ERRORLOG=${WORKINGDIR}/error.log
INSTALLLOG=${WORKINGDIR}/install.log
echoinstall_var WORKINGDIR "silent"
echoinstall_var ERRORLOG "silent"
echoinstall_var INSTALLLOG "silent"
echo > ${ERRORLOG}
echo > ${INSTALLLOG}

# process user input
while :; do
  case $1 in
    -h|-\?|--help)
      print_usage
      exit
      ;;
    -d|--install-path)
      optarg_read_var "OPT_INSTALL_PATH" $2 $1
      shift
      ;;
    -l|--license-type)
      optarg_read_var "OPT_LICENSE_TYPE" $2 $1 "^(community|web-floating|static|floating-server|floating-client)$"
      shift
      ;;
    -s|--license-server-name)
      optarg_read_var "OPT_LICENSE_SERVER" $2 $1
      shift
      ;;
    -a|--xpauth-path)
      optarg_read_var "OPT_XPAUTH_PATH" $2 $1
      shift
      ;;
    -o|--no-interactive)
      optarg_var_enable "OPT_NO_INTERACTIVE"
      ;;
    -p|--include-in-bashrc)
      optarg_var_enable "OPT_INCLUDE_IN_BASHRC"
      ;;
    -k|--accept-kalis-license)
      optarg_var_enable "OPT_KALIS_LICENSE"
      ;;
    -x|--accept-xpress-license)
      optarg_var_enable "OPT_XPRESS_LICENSE"
      ;;
    -c|--components)
      optarg_read_var "OPT_COMPONENTS" $2 $1
      shift
      ;;
    -?*)
      echo "Error: could not parse command-line parameter $1."
      print_usage
      exit 1
      ;;
    *)
      break
  esac
  shift
done

INTERACTIVE_MODE=yes
if [ "$OPT_NO_INTERACTIVE" = "yes" ]; then
  INTERACTIVE_MODE=no
fi

echoinstall "FICO Xpress installation utility"
echo

choose_setup

setup_installation

# only start license manager if installed a distributed server and user has given a valid license.
if [ "$PACKAGE_TYPE" = "distrib_server" ] && [ ! -z "$LICPATH" ]; then
  start_lmgrd # Start license manager.
fi

if [ "$PACKAGE_TYPE" != "distrib_server" ]; then
  generate_xprmsrv_key # Generate an xprmsrv key.
fi

# Assign non-relative paths to all Python modules if this is a MacOS install
if [[ "$(basename $PKGFILE .tar.gz | cut -f 2- -d _)" =~ macos ]]; then
  if [ -d "${XPRESSDIR}/lib/xpress.*" ]; then
    for i in ${XPRESSDIR}/lib/xpress.*; do
      install_name_tool -change libxprs.dylib ${XPRESSDIR}/lib/libxprs.dylib $i
    done
  fi
fi

echoinstall "Installation complete!"
echo
echoinstall "If you use a Bourne shell, set up your environment to use FICO Xpress by running:"
echoinstall "  . $XPRESSDIR/bin/xpvars.sh"
if [ "$INCLUDE_IN_BASHRC" = "yes" ]; then
  echoinstall "(this will be done automatically when a new Bash shell is created)"
fi
echoinstall "Or if you use a C shell, run:"
echoinstall "  source $XPRESSDIR/bin/xpvars.csh"
echo

if [ "$NEED_LIC" = "pc" ]; then
  echoinstall "When you have received your license file, set the XPAUTH_PATH environment variable"
  echoinstall "to point to it, e.g.:"
  echoinstall "csh% setenv XPAUTH_PATH $XPRESSDIR/bin/xpauth.xpr"
  echoinstall "or:"
  echoinstall "sh$ export XPAUTH_PATH=$XPRESSDIR/bin/xpauth.xpr"
  echo
elif [ "$NEED_LIC" = "distrib" ]; then
  echoinstall "When you have received your license file, start the license manager, e.g.:"
  echoinstall "  xpserver -d -xpress $XPRESSDIR/bin -logfile /var/tmp/xpress.log"
  echo
fi

echoinstall "For more information about getting started with FICO Xpress please read"
echoinstall "$XPRESSDIR/readme.html."
echo
