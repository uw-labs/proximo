package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v2"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
)

type ACLConfig struct {
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

type Scope struct {
	Consume []*regexp.Regexp
	Publish []*regexp.Regexp
}

type aclStore struct {
	auth         map[string][]byte
	defaultScope Scope
	scopes       map[string]*Scope
}

func ConfigFromFile(configFile string) (ACLConfig, error) {
	var conf ACLConfig

	dat, err := ioutil.ReadFile(configFile)
	if err != nil {
		return conf, err
	}

	err = yaml.Unmarshal(dat, &conf)
	if err != nil {
		return conf, err
	}

	return conf, nil
}

type ACLFactory struct {
	store  *aclStore
	source proximo.AsyncSourceFactory
	sink   proximo.AsyncSinkFactory
}

func (s *ACLFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	scope, err := s.getScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Consume, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.source.NewAsyncSource(ctx, req)
}

func (s *ACLFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	scope, err := s.getScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.sink.NewAsyncSink(ctx, req)
}

func (s *ACLFactory) getScope(ctx context.Context) (*Scope, error) {
	val := metautils.ExtractIncoming(ctx).Get("authorization")
	if val == "" {
		return &s.store.defaultScope, nil
	}

	basicAuth, err := grpc_auth.AuthFromMD(ctx, "Bearer")
	if err != nil {
		return nil, ErrUnauthorized
	}

	payload, err := base64.StdEncoding.DecodeString(basicAuth)
	if err != nil {
		return nil, err
	}

	pair := strings.SplitN(string(payload), ":", 2)

	if len(pair) != 2 {
		return nil, ErrUnauthorized
	}

	id, secret := pair[0], pair[1]

	hashPass, ok := s.store.auth[id]
	if !ok {
		return nil, ErrUnauthorized
	}

	if err := bcrypt.CompareHashAndPassword(hashPass, []byte(secret)); err != nil {
		return nil, ErrUnauthorized
	}

	return s.store.scopes[id], nil
}

func ProximoACLSourceFactory(config ACLConfig, factory proximo.AsyncSourceFactory) (proximo.AsyncSourceFactory, error) {
	store, err := compileConfig(config)
	if err != nil {
		return nil, err
	}

	return &ACLFactory{
		store:  store,
		source: factory,
	}, nil
}

func ProximoACLSinkFactory(config ACLConfig, factory proximo.AsyncSinkFactory) (proximo.AsyncSinkFactory, error) {
	store, err := compileConfig(config)
	if err != nil {
		return nil, err
	}

	return &ACLFactory{
		store: store,
		sink:  factory,
	}, nil
}

func compileConfig(config ACLConfig) (*aclStore, error) {
	auth := make(map[string][]byte)
	scopes := make(map[string]*Scope)

	roles, err := compileRoles(config)
	if err != nil {
		return nil, err
	}

	defaultScope, err := compileScopes(config.Default.Roles, roles)
	if err != nil {
		return nil, err
	}

	for _, c := range config.Clients {
		auth[c.ID] = []byte(c.Secret)

		s, err := compileScopes(append(config.Default.Roles, c.Roles...), roles)
		if err != nil {
			return nil, err
		}

		scopes[c.ID] = s
	}

	return &aclStore{
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

func compileRoles(config ACLConfig) (map[string]*Scope, error) {
	roles := make(map[string]*Scope)

	for _, r := range config.Roles {
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
