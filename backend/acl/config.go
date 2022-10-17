package acl

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

var (
	// ErrUnauthorized indicates that a client doesn't have access to a resource
	ErrUnauthorized = status.Errorf(codes.PermissionDenied, "unauthorised")
)

// Config allows retrieve of access scope for the current client
type Config interface {
	GetClientScope(ctx context.Context) (*Scope, error)
}

// Scope contains the access rights for a particular client
type Scope struct {
	Consume []*regexp.Regexp
	Publish []*regexp.Regexp
}

type configYaml struct {
	Default struct {
		Roles []string `yaml:"roles"`
	} `yaml:"default"`
	Roles []struct {
		ID      string   `yaml:"id"`
		Consume []string `yaml:"consume,omitempty"`
		Publish []string `yaml:"publish,omitempty"`
	} `yaml:"roles"`
	Clients []struct {
		ID     string   `yaml:"id"`
		Secret string   `yaml:"secret"`
		Roles  []string `yaml:"roles"`
	} `yaml:"clients"`
}

type config struct {
	auth         map[string][]byte
	defaultScope Scope
	scopes       map[string]*Scope
}

// ConfigFromFile parses and compiles ACL config from a yaml file
func ConfigFromFile(configFile string) (Config, error) {
	var conf configYaml

	dat, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(dat, &conf)
	if err != nil {
		return nil, err

	}

	store, err := compileConfig(conf)
	if err != nil {
		return nil, err
	}

	return store, nil
}

// ConfigFromString parses and compiles ACL config from a string
func ConfigFromString(configString string) (Config, error) {
	var conf configYaml

	err := yaml.Unmarshal([]byte(configString), &conf)
	if err != nil {
		return nil, err

	}

	store, err := compileConfig(conf)
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (s *config) GetClientScope(ctx context.Context) (*Scope, error) {
	val := metautils.ExtractIncoming(ctx).Get("authorization")
	if val == "" {
		return &s.defaultScope, nil
	}

	basicAuth, err := grpc_auth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, ErrUnauthorized
	}

	payload, err := base64.StdEncoding.DecodeString(basicAuth)
	if err != nil {
		return nil, ErrUnauthorized
	}

	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 {
		return nil, ErrUnauthorized
	}

	id, secret := pair[0], pair[1]

	hashPass, ok := s.auth[id]
	if !ok {
		return nil, ErrUnauthorized
	}

	if err := bcrypt.CompareHashAndPassword(hashPass, []byte(secret)); err != nil {
		return nil, ErrUnauthorized
	}

	return s.scopes[id], nil
}

func compileConfig(yamlCfg configYaml) (*config, error) {
	auth := make(map[string][]byte)
	scopes := make(map[string]*Scope)

	roles, err := compileRoles(yamlCfg)
	if err != nil {
		return nil, err
	}

	defaultScope, err := compileScopes(yamlCfg.Default.Roles, roles)
	if err != nil {
		return nil, err
	}

	for _, c := range yamlCfg.Clients {
		auth[c.ID] = []byte(c.Secret)

		s, err := compileScopes(append(yamlCfg.Default.Roles, c.Roles...), roles)
		if err != nil {
			return nil, err
		}

		scopes[c.ID] = s
	}

	return &config{
		auth:         auth,
		defaultScope: *defaultScope,
		scopes:       scopes,
	}, nil
}

func compileScopes(roles []string, rolesMap map[string]*Scope) (*Scope, error) {
	result := &Scope{}

	for _, r := range roles {
		s, ok := rolesMap[r]
		if !ok {
			return nil, fmt.Errorf("undefined role %s", r)
		}

		result.Consume = append(result.Consume, s.Consume...)
		result.Publish = append(result.Publish, s.Publish...)
	}

	return result, nil
}

func compileRoles(cfg configYaml) (map[string]*Scope, error) {
	roles := make(map[string]*Scope)

	for _, r := range cfg.Roles {
		s := &Scope{}

		for _, c := range r.Consume {
			re, err := regexp.Compile(c)
			if err != nil {
				return nil, err
			}

			s.Consume = append(s.Consume, re)
		}

		for _, c := range r.Publish {
			re, err := regexp.Compile(c)
			if err != nil {
				return nil, err
			}

			s.Publish = append(s.Publish, re)
		}

		roles[r.ID] = s
	}

	return roles, nil
}

func containsRegex(list []*regexp.Regexp, s string) bool {
	for _, i := range list {
		if i.Match([]byte(s)) {
			return true
		}
	}

	return false
}
