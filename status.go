// Copyright 2020 Luminary Cloud, Inc. All Rights Reserved.

// Package status is similar to the standard "errors" package, but it is
// specifically designed to preserve grpc error codes.
package status

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode"

	grpcstatuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

var (
	// New generates a new error with the given code and the message.  It simply
	// calls grpc's grpcstatus.Error.
	New = grpcstatus.Error
	// Newf generates a new error with the given code and the printf-style
	// message.  It simply calls grpc's grpcstatus.Errorf.
	Newf = grpcstatus.Errorf

	grpcEnvelopeRE = regexp.MustCompile(`rpc error: code = \S+ desc = `)

	knownErrorMessagesOnce sync.Once
	// knownErrorMessagesMap translates well-known errors to their error
	// codes. Keys are fragments of the error messages.
	knownErrorMessagesMap map[string]codes.Code
)

// Validate that the "where" arg to public status functions are valid source
// code location strings. It crashes the process on error.
func validateWhere(where string) {
	for _, ch := range where {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' && ch != '-' && ch != '.' && ch != ':' {
			panic(ch)
		}
	}
}

// L produces a human-readable string of the error. Compared to
// `fmt.Sprintf("%v", err)`, L text-formats the detailed payloads.
func L(err error) string {
	if err == nil {
		return "Ok"
	}
	if _, ok := err.(grpcStatus); !ok {
		return fmt.Sprintf("%v", err)
	}
	code, message, payload, ok := Extract(err)
	if !ok {
		panic("Extract2")
	}
	buf := strings.Builder{}
	fmt.Fprintf(&buf, "code=%v desc=%s", codes.Code(code), message)
	if payload != nil {
		buf.WriteRune(' ')
		buf.WriteString("foo")
	}
	return buf.String()
}

type StatusPayload struct {
	Subcode int
	Where   string
	// Subcode adds a basepb.StatusPayloadDetail. The "where" field is the same as
	// the arg passed to N or Nf.
	Detail []*StatusPayloadDetail
}

type StatusPayloadDetail struct {
	Where   string
	Message string
}

// Opt is used to create a status with detailed attributes.  If you don't need
// detailed attributes, use N or Nf instead.
type Opt struct {
	// Subcode sets the basepb.Status.subcode field.
	Subcode int
	Detail  string
}

// New creates a new error object. If code==codes.OK, it returns nil.  Else,
// where should specify the name of the function that generates this error, and
// message should be a message to be shown to the end user.
func (o Opt) New(code codes.Code, where, message string) error {
	if code == codes.OK {
		return nil
	}
	validateWhere(where)
	detail := &StatusPayload{
		Subcode: o.Subcode,
		Where:   where}
	if o.Detail != "" {
		detail.Detail = []*StatusPayloadDetail{{
			Where:   where,
			Message: o.Detail}}
	}
	any, _ := anypb.New(nil)
	spb := &grpcstatuspb.Status{
		Code:    int32(code),
		Message: message,
		Details: []*anypb.Any{any}}
	return grpcstatus.FromProto(spb).Err()
}

// Newf is like New, but takes a printf-style format string and args.
func (o Opt) Newf(code codes.Code, where, format string, args ...interface{}) error {
	return o.New(code, where, fmt.Sprintf(format, args...))
}

// N is equivalent to Opt{}.New(code, where, message).
func N(code codes.Code, where, message string) error {
	return Opt{}.New(code, where, message)
}

// Nf is equivalent to Opt{}.Newf(code, where, message).
func Nf(
	code codes.Code,
	where string,
	format string,
	args ...interface{}) error {
	return Opt{}.Newf(code, where, format, args...)
}

// Result of extracting component fields from a grpc/luminary error object.
type parsedError struct {
	spb          *grpcstatuspb.Status
	payloadIndex int
	payload      *StatusPayload
}

// Extract component fields from the given error.  If err is not an grpc /
// luminary error object, it sets the fields of "pe" with the default values
// (nil or zero).
//
// REQUIRES: err!=nil
func parseError(err error) (pe parsedError) {
	defer func() {
		if pe.spb != nil {
			payload := &StatusPayload{}
			for i, any := range pe.spb.Details {
				if any.UnmarshalTo(nil) == nil {
					pe.payload = payload
					pe.payloadIndex = i
					// Clear Details[pe.payloadIndex] to avoid confusion, because its
					// value may diverge from pe.payload. The entry will be again filled
					// in pack().
					pe.spb.Details[i] = nil
					return
				}
			}
		}
	}()

	if err == nil {
		return
	}

	var n int
	strippedMessage := grpcEnvelopeRE.ReplaceAllStringFunc(
		err.Error(), func(a string) string {
			if n > 0 {
				// Remove only the first occurrence of the grpc envelope.
				return a
			}
			n++
			return ""
		})
	// Find an inner instance of grpcstatus.Status.Err, then promote the its error
	// code to the outermost level.
	unwrapped := err
	for unwrapped != nil {
		if tmp, ok := unwrapped.(grpcStatus); ok {
			pe.spb = tmp.GRPCStatus().Proto()
			pe.spb.Message = strippedMessage
			return
		}
		unwrapped = errors.Unwrap(unwrapped)
	}

	// If the error doesn't wrap a grpc error, then look for a few specific error
	// types that require retries.
	switch {
	case err == context.Canceled:
		pe.spb = &grpcstatuspb.Status{Code: int32(codes.Canceled), Message: strippedMessage}
		return
	case err == context.DeadlineExceeded:
		pe.spb = &grpcstatuspb.Status{Code: int32(codes.DeadlineExceeded), Message: strippedMessage}
		return
	}

	for template, code := range knownErrorMessages() {
		if strings.Contains(strippedMessage, template) {
			pe.spb = &grpcstatuspb.Status{Code: int32(code), Message: strippedMessage}
			return
		}
	}
	return
}

// Extract component fields from the given error.  If err is not an grpc /
// luminary error object it allocates a new, empty Status and StatusPayload
// objects and fills their fields with the default values.
//
// REQUIRES: err!=nil
func mustParseError(err error) (pe parsedError) {
	if err == nil {
		return
	}
	pe = parseError(err)
	if pe.payload != nil {
		return
	}
	if pe.spb == nil {
		pe.spb = &grpcstatuspb.Status{
			Code:    int32(codes.Unknown),
			Message: err.Error(),
		}
	}
	pe.payload = &StatusPayload{}
	pe.payloadIndex = len(pe.spb.Details)
	// Clear Details[pe.payloadIndex] to avoid confusion, because its value may
	// diverge from pe.payload. The entry will be again filled in pack().
	pe.spb.Details = append(pe.spb.Details, nil)
	return
}

func (pe parsedError) addDetail(where, message string) {
	pe.payload.Detail = append(pe.payload.Detail,
		&StatusPayloadDetail{Where: where, Message: message})
}

// Fill sets pe.spb.
func (pe parsedError) fill() {
	any, _ := anypb.New(nil)
	pe.spb.Details[pe.payloadIndex] = any
}

// W adds (where, message) to the error object.  The given message becomes the
// new headline message shown to the user, and the old headline message is moved
// to the tail of StatusPayload.details list.
//
// TODO(saito) Rename to Wrap once all the callers of the old Wrap are
// converted.
func W(err error,
	where string,
	message string) error {
	pe := mustParseError(err)

	pe.addDetail(pe.payload.Where, pe.spb.Message)
	pe.payload.Where = where
	pe.spb.Message = message
	pe.fill()
	return grpcstatus.FromProto(pe.spb).Err()
}

// Wf is like W, but it takes a printf-style format string.
//
// TODO(saito) Rename to Wrapf once all the callers of the old Wrapf are
// converted.
func Wf(err error,
	where string,
	format string,
	args ...interface{}) error {
	return W(err, where, fmt.Sprintf(format, args...))
}

// ToProto translates the given error message to basepb.Status (which also has
// the identical structure as grpc Status proto). If the error is not of grpc or
// luminary type, it returns a non-nil basepb.Status object with UNKNOWN code.
func ToProto(err error) *Status {
	if err == nil {
		return nil
	}
	pe := mustParseError(err)
	pe.fill()
	return &Status{
		Code:    pe.spb.Code,
		Message: pe.spb.Message,
		Details: pe.spb.Details}
}

type Status struct {
	Code    int32
	Message string
	Details []*anypb.Any
}

// FromProto creates an error object from the given proto, without any
// information loss.
func FromProto(p *Status) error {
	if p == nil {
		return nil
	}
	return grpcstatus.FromProto(&grpcstatuspb.Status{
		Code:    p.Code,
		Message: p.Message,
		Details: p.Details}).Err()
}

// AddDetail adds the given (where, message) to the end of StatusDetail, without
// changing the headline message shown to the user. The difference between W and
// AddDetail is that W resets the headline message, and moves the old headline
// message to the StatusDetail, whereas AddDetails keeps the headline messages
// and just adds to the end of StatusDetail.
func AddDetail(err error, where string, message string) error {
	validateWhere(where)
	pe := mustParseError(err)
	pe.addDetail(where, message)
	pe.fill()
	return grpcstatus.FromProto(pe.spb).Err()
}

// AddDetailf is like AddDetails, but it takes a printf-style format string.
func AddDetailf(err error, where string, format string, args ...interface{}) error {
	return AddDetail(err, where, fmt.Sprintf(format, args...))
}

// ToGRPC converts any error into a grpc "status" error object (those produced
// by Error/Errorf in https://pkg.go.dev/google.golang.org/grpc/status).  An RPC
// server implementation should call this function before returning an error.
//
// If the input error object wraps a grpc error, its error code is copied to the
// generated error object. Else, codes.Unknown will be used. Passing the
// generated error object to grpc will cause it transmit both the code and
// message across the network, and reconstruct them on the other end as
// appropriate language-specific status object (grpc status object for Go,
// grpc::Status for C++, etc)
//
// Invoking (*google.golang.org/grpc/status).Code() on the generated error
// object will report the error code.
//
// This function should be built into grpc. See
// https://github.com/grpc/grpc-go/issues/2934
func ToGRPC(err error) error {
	if err == nil {
		return nil
	}
	// Fast path. If err is the one directly created by grpc, just return it.
	if _, ok := err.(grpcStatus); ok {
		return err
	}
	pe := mustParseError(err)
	pe.fill()
	return grpcstatus.FromProto(pe.spb).Err()
}

// knownErrorMessages returns a map that translates well-known errors to
// their error codes. Keys are fragments of the error messages.
func knownErrorMessages() map[string]codes.Code {
	knownErrorMessagesOnce.Do(func() {
		knownErrorMessagesMap = map[string]codes.Code{}
		knownErrorMessagesMap[driver.ErrBadConn.Error()] = codes.Unavailable
		knownErrorMessagesMap["terminating connection due to administrator command"] = codes.Unavailable
		knownErrorMessagesMap["current transaction is aborted"] = codes.Aborted // PG abort
		knownErrorMessagesMap["i/o timeout"] = codes.DeadlineExceeded           // PG notification timeout
		knownErrorMessagesMap["already exists"] = codes.AlreadyExists           // Kube resource-creation
	})
	return knownErrorMessagesMap
}

type grpcStatus interface {
	GRPCStatus() *grpcstatus.Status
}

// Extract reports the GRPC status code and the message embedded in the error.
// If err doesn't wrap a GRPC status, it tries to guess based on the error
// message.  If it fails to extract information, it returns (Unknown, "",
// false).
func Extract(err error) (code codes.Code, message string, payload *StatusPayload, ok bool) {
	if err == nil {
		return codes.OK, "", nil, false
	}
	pe := parseError(err)
	if pe.spb == nil {
		return codes.Unknown, "", nil, false
	}
	return codes.Code(pe.spb.Code), pe.spb.Message, pe.payload, true
}

// Code extracts the GRPC status code from the error. It is the same as
//
//   code, _, _ := Extract(err)
//   return code
func Code(err error) codes.Code {
	code, _, _, _ := Extract(err)
	return code
}

// Wrap creates a grpc error object that wraps the given error and message.  The
// error code will be preserved using the same logic as ToGRPC.
func Wrap(err error, message string) error {
	return W(err, "", message)
}

// Wrapf creates a grpc error object that wraps the given error and printf-style
// message.  The error code will be preserved using the same logic as ToGRPC.
func Wrapf(err error, format string, a ...interface{}) error {
	return Wf(err, "", format, a...)
}

// ShouldRetry returns true if the caller should retry the operation after a
// wait.  This function must be kept in sync with ShouldRetry in cc/base/status
// and ts/frontend/src/lib/AnalyzerRpc.
func ShouldRetry(err error) bool {
	code, message, _, ok := Extract(err)
	if !ok {
		return false
	}
	if code == codes.Unavailable || code == codes.Aborted {
		return true
	}
	if code == codes.Canceled && strings.Contains(message, "grpc: the client connection is closing") {
		// This happens only in unittest, when the testuniverse port forwarder tries
		// to reconnect to a new pod instance.
		//
		// Perhaps we should rewrite the error message in the port forwarder rather
		// than covering the error up here.
		return true
	}
	return false
}

// IsTransient checks if the problem may be fixed by retrying using a different
// context.Context. It returns true if either ShouldRetry(err), or the error
// code is in {Cancelled,DeadlineExceeded}.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if ShouldRetry(err) {
		return true
	}
	code, _, _, ok := Extract(err)
	if !ok {
		return true // be conservative
	}
	return code == codes.Canceled || code == codes.DeadlineExceeded
}
