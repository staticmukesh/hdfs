package main

import (
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/pborman/getopt"
	"gopkg.in/jcmturner/gokrb5.v3/client"
	"gopkg.in/jcmturner/gokrb5.v3/config"
	"gopkg.in/jcmturner/gokrb5.v3/keytab"
	"log"
	"os"
	"strings"
)

// TODO: cp, tree, test, trash

const hdfsDefaultServiceName = "nn"
const krbDefaultCfgPath = "/etc/krb5.conf"

var (
	version string
	usage   = fmt.Sprintf(`Usage: %s COMMAND
The flags available are a subset of the POSIX ones, but should behave similarly.

Valid commands:
  ls [-lah] [FILE]...
  rm [-rf] FILE...
  mv [-nT] SOURCE... DEST
  mkdir [-p] FILE...
  touch [-amc] FILE...
  chmod [-R] OCTAL-MODE FILE...
  chown [-R] OWNER[:GROUP] FILE...
  cat SOURCE...
  head [-n LINES | -c BYTES] SOURCE...
  tail [-n LINES | -c BYTES] SOURCE...
  du [-sh] FILE...
  checksum FILE...
  get SOURCE [DEST]
  getmerge SOURCE DEST
  put SOURCE DEST
  df [-h]

Environment variables to set : 
  - HADOOP_KEYTAB=<path_to_keytab>
  - HADOOP_KRB_CONF=<path_to_krb_conf>
  - HADOOP_NAMENODE=<namenode1>:<port>,<namenode2>:<port>

`, os.Args[0])

	lsOpts = getopt.New()
	lsl    = lsOpts.Bool('l')
	lsa    = lsOpts.Bool('a')
	lsh    = lsOpts.Bool('h')

	rmOpts = getopt.New()
	rmr    = rmOpts.Bool('r')
	rmf    = rmOpts.Bool('f')

	mvOpts = getopt.New()
	mvn    = mvOpts.Bool('n')
	mvT    = mvOpts.Bool('T')

	mkdirOpts = getopt.New()
	mkdirp    = mkdirOpts.Bool('p')

	touchOpts = getopt.New()
	touchc    = touchOpts.Bool('c')

	chmodOpts = getopt.New()
	chmodR    = chmodOpts.Bool('R')

	chownOpts = getopt.New()
	chownR    = chownOpts.Bool('R')

	headTailOpts = getopt.New()
	headtailn    = headTailOpts.Int64('n', -1)
	headtailc    = headTailOpts.Int64('c', -1)

	duOpts = getopt.New()
	dus    = duOpts.Bool('s')
	duh    = duOpts.Bool('h')

	getmergeOpts = getopt.New()
	getmergen    = getmergeOpts.Bool('n')

	dfOpts = getopt.New()
	dfh    = dfOpts.Bool('h')

	cachedClient *hdfs.Client
	status       = 0
)

func init() {
	lsOpts.SetUsage(printHelp)
	rmOpts.SetUsage(printHelp)
	mvOpts.SetUsage(printHelp)
	touchOpts.SetUsage(printHelp)
	chmodOpts.SetUsage(printHelp)
	chownOpts.SetUsage(printHelp)
	headTailOpts.SetUsage(printHelp)
	duOpts.SetUsage(printHelp)
	getmergeOpts.SetUsage(printHelp)
	dfOpts.SetUsage(printHelp)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	command := os.Args[1]
	argv := os.Args[1:]
	switch command {
	case "-v", "--version":
		fatal("gohdfs version", version)
	case "ls":
		lsOpts.Parse(argv)
		ls(lsOpts.Args(), *lsl, *lsa, *lsh)
	case "rm":
		rmOpts.Parse(argv)
		rm(rmOpts.Args(), *rmr, *rmf)
	case "mv":
		mvOpts.Parse(argv)
		mv(mvOpts.Args(), !*mvn, *mvT)
	case "mkdir":
		mkdirOpts.Parse(argv)
		mkdir(mkdirOpts.Args(), *mkdirp)
	case "touch":
		touchOpts.Parse(argv)
		touch(touchOpts.Args(), *touchc)
	case "chown":
		chownOpts.Parse(argv)
		chown(chownOpts.Args(), *chownR)
	case "chmod":
		chmodOpts.Parse(argv)
		chmod(chmodOpts.Args(), *chmodR)
	case "cat":
		cat(argv[1:])
	case "head", "tail":
		headTailOpts.Parse(argv)
		printSection(headTailOpts.Args(), *headtailn, *headtailc, (command == "tail"))
	case "du":
		duOpts.Parse(argv)
		du(duOpts.Args(), *dus, *duh)
	case "checksum":
		checksum(argv[1:])
	case "get":
		get(argv[1:])
	case "getmerge":
		getmergeOpts.Parse(argv)
		getmerge(getmergeOpts.Args(), *getmergen)
	case "put":
		put(argv[1:])
	case "df":
		dfOpts.Parse(argv)
		df(*dfh)
	// it's a seeeeecret command
	case "complete":
		complete(argv)
	case "help", "-h", "-help", "--help":
		printHelp()
	default:
		fatalWithUsage("Unknown command:", command)
	}

	os.Exit(status)
}

func printHelp() {
	fmt.Fprintln(os.Stderr, usage)
	os.Exit(0)
}

func fatal(msg ...interface{}) {
	fmt.Fprintln(os.Stderr, msg...)
	os.Exit(1)
}

func fatalWithUsage(msg ...interface{}) {
	msg = append(msg, "\n"+usage)
	fatal(msg...)
}

// getClient returns a HDFS client to the namenode or namenods provided.
// if an empty string is provided, the env var HADOOP_NAMENODE is looked up.
// one or multiple namenodes may be specified in a comma separated list: "<namenode1>:<port>,<namenode2>:<port>,..."
func getClient(namenodes string) (*hdfs.Client, error) {
	if cachedClient != nil {
		return cachedClient, nil
	}

	if namenodes == "" {
		namenodes = os.Getenv("HADOOP_NAMENODE")
	}

	if namenodes == "" && os.Getenv("HADOOP_CONF_DIR") == "" {
		return nil, errors.New("Couldn't find a namenode to connect to. You should specify hdfs://<namenode>:<port> in your paths. Alternatively, set HADOOP_NAMENODE or HADOOP_CONF_DIR in your environment.")
	}

	options := hdfs.ClientOptions{}
	// TODO: HA failover ?!
	options.Addresses = strings.Split(namenodes, ",")
	// Sets the kerberos client only if the relevant settings are set
	options.KerberosClient = getKrbClientIfRequired()
	options.ServicePrincipalName = getServiceName()

	c, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	cachedClient = c

	return cachedClient, nil
}

// getServiceName returns 'nn' unless the HADOOP_SNAME environment variable
func getServiceName() string {
	if sn := os.Getenv("HADOOP_SNAME"); sn != "" {
		return sn
	}
	return hdfsDefaultServiceName
}

// getKrbClientIfRequired returns a client if the environment variables suggest a client is required.
// If HADOOP_KEYTAB is set, a kerberized cluster is assumed.
func getKrbClientIfRequired() *client.Client {
	keytabPath := os.Getenv("HADOOP_KEYTAB")

	if keytabPath == "" {
		// Nothing to do
		return nil
	}

	var krb5Cfg = os.Getenv("HADOOP_KRB_CONF")

	if krb5Cfg == "" {
		krb5Cfg = krbDefaultCfgPath
	}

	return getKrbClientAndLogin(krb5Cfg, keytabPath)
}

func getKrbClientAndLogin(configPath string, keytabPath string) *client.Client {

	cfg, cfgE := config.Load(configPath)

	if cfgE != nil {
		log.Fatal(cfgE)
	}

	kt, ktE := keytab.Load(keytabPath)

	if ktE != nil {
		log.Fatal(ktE)
	}

	entries := kt.Entries

	if len(entries) == 0 {
		log.Fatalf("no entries found in keytab %s" + keytabPath)
	}

	// Fetch the principal of the first entry
	principal := entries[0].Principal

	cl := client.NewClientWithKeytab(strings.Join(principal.Components, "/"), principal.Realm, kt)
	cl.WithConfig(cfg)

	// TODO Config flag or whatever for people not using AD
	cl.GoKrb5Conf.DisablePAFXFast = true
	if loginE := cl.Login(); loginE != nil {
		log.Fatal(loginE)
	}
	return &cl
}
