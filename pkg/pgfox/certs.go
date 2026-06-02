package pgfox

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// loadCA loads the CA certificate and key from the fixed paths under PgFoxDir.
// CA cert: {pgfox_dir}/ca.crt
// CA key:  {pgfox_dir}/ca.key
func (p *Server) loadCA() (*x509.Certificate, *rsa.PrivateKey, error) {
	caCertPEM, err := os.ReadFile(p.caCertPath())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CA cert %s: %w", p.caCertPath(), err)
	}

	caKeyPEM, err := os.ReadFile(p.caKeyPath())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CA key %s: %w", p.caKeyPath(), err)
	}

	// Parse CA certificate
	block, _ := pem.Decode(caCertPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("failed to decode CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA cert: %w", err)
	}

	// Parse CA private key
	keyBlock, _ := pem.Decode(caKeyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("failed to decode CA key PEM")
	}
	caKey, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		// Try PKCS1 format as fallback
		rsaKey, err2 := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
		if err2 != nil {
			return nil, nil, fmt.Errorf("failed to parse CA key (tried PKCS8 and PKCS1): %w", err)
		}
		return caCert, rsaKey, nil
	}

	rsaKey, ok := caKey.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("CA key is not an RSA key")
	}

	return caCert, rsaKey, nil
}

// loadCACertPool returns an x509.CertPool containing only the PgFox CA.
// Used for verify-full TLS connections to the backend.
func (p *Server) loadCACertPool() (*x509.CertPool, error) {
	caCertPEM, err := os.ReadFile(p.caCertPath())
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCertPEM) {
		return nil, fmt.Errorf("failed to parse CA cert into Pool")
	}

	return pool, nil
}

// loadOrGenerateUserCert returns a TLS certificate for the given username.
// It checks the cert cache at {pgfox_dir}/certs/{username}.crt:
//   - If the cert exists, is signed by the current CA, and is not expired → use it
//   - Otherwise → generate a new cert signed by the CA, cache it, and use it
func (p *Server) loadOrGenerateUserCert(username string) (tls.Certificate, error) {
	certPath := p.userCertPath(username)
	keyPath := p.userKeyPath(username)

	// Check if a valid cached cert exists. User certs don't need SANs —
	// PostgreSQL matches the client cert CN to the role name.
	if p.isCertValidForCA(certPath, false) {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err == nil {
			p.Logger.Debug("Using cached user certificate", "user", username, "cert", certPath)
			return cert, nil
		}
		// Key or cert unreadable — regenerate
		p.Logger.Debug("Cached cert unreadable, regenerating", "user", username, "err", err)
	}

	return p.generateAndCacheUserCert(username)
}

// isCertValidForCA checks whether a certificate file at certPath:
//  1. Exists and is readable
//  2. Is signed by the current CA
//  3. Has not expired (with a 24h buffer before expiry to allow renewal)
//
// requireSAN should be true for server/TLS certificates (Go 1.15+ requires SANs
// for hostname verification) and false for client certificates (user certs use
// CN for PostgreSQL role matching and do not need SANs).
func (p *Server) isCertValidForCA(certPath string, requireSAN bool) bool {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return false
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return false
	}

	// Check expiry — renew if within 24 hours of expiry.
	if time.Now().Add(24 * time.Hour).After(cert.NotAfter) {
		return false
	}

	// Server certs require SANs for Go 1.15+ hostname verification.
	// User (client) certs do not — PostgreSQL matches on CN only.
	if requireSAN && len(cert.IPAddresses) == 0 && len(cert.DNSNames) == 0 {
		return false
	}

	// Verify signed by current CA.
	caCertPEM, err := os.ReadFile(p.caCertPath())
	if err != nil {
		return false
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCertPEM) {
		return false
	}

	opts := x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	_, err = cert.Verify(opts)
	return err == nil
}

// generateAndCacheUserCert generates a new TLS client certificate for the given
// username, signs it with the CA, caches it to disk, and returns it.
// The certificate CN is set to the username for PostgreSQL cert auth.
func (p *Server) generateAndCacheUserCert(username string) (tls.Certificate, error) {
	p.Logger.Info("Generating user certificate", "user", username)

	caCert, caKey, err := p.loadCA()
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to load CA for cert generation: %w", err)
	}

	// Generate RSA key for the user
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate RSA key for %s: %w", username, err)
	}

	// Build certificate template
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial number: %w", err)
	}

	cfg := p.Config.Server.Certs
	now := time.Now()

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         username, // PostgreSQL matches CN to role name
			Organization:       []string{cfg.Organization},
			OrganizationalUnit: []string{cfg.OrganizationalUnit},
			Country:            []string{cfg.Country},
		},
		NotBefore:             now.Add(-1 * time.Minute), // small back-date for clock skew
		NotAfter:              now.Add(cfg.TTL),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// Sign with CA
	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create certificate for %s: %w", username, err)
	}

	// Ensure certs directory exists
	certsDir := p.certsDir()
	if err := os.MkdirAll(certsDir, 0700); err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create certs dir %s: %w", certsDir, err)
	}

	certPath := p.userCertPath(username)
	keyPath := p.userKeyPath(username)

	// Write certificate
	certFile, err := os.OpenFile(certPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to write cert file %s: %w", certPath, err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certFile.Close()
		return tls.Certificate{}, fmt.Errorf("failed to encode cert PEM: %w", err)
	}
	certFile.Close()

	// Write private key
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to marshal private key: %w", err)
	}

	keyFile, err := os.OpenFile(keyPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to write key file %s: %w", keyPath, err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}); err != nil {
		keyFile.Close()
		return tls.Certificate{}, fmt.Errorf("failed to encode key PEM: %w", err)
	}
	keyFile.Close()

	p.Logger.Info("User certificate generated and cached",
		"user", username,
		"cert", certPath,
		"expires", template.NotAfter.Format(time.RFC3339))

	// Load and return the final tls.Certificate
	return tls.LoadX509KeyPair(certPath, keyPath)
}

// backendTLSConfig builds the tls.Config for a backend connection.
// Always uses verify-full with client certificate authentication.
// clientCert is the per-user certificate to present to the backend.
func (p *Server) backendTLSConfig(host string, clientCert tls.Certificate) (*tls.Config, error) {
	caPool, err := p.loadCACertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to load CA Pool for backend TLS: %w", err)
	}

	return &tls.Config{
		ServerName:   host,
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
		MinVersion:   tls.VersionTLS12,
		// verify-full: verify server cert against CA AND check hostname
		InsecureSkipVerify: false,
	}, nil
}

// Path helper methods — all paths derived from pgfox_dir

func (p *Server) caCertPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "ca.crt")
}

func (p *Server) caKeyPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "ca.key")
}

// serverCertPath returns the path for the PostgreSQL server certificate.
// Operators copy this to $PGDATA and set ssl_cert_file in postgresql.conf.
func (p *Server) serverCertPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "server.crt")
}

func (p *Server) serverKeyPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "server.key")
}

// pgfoxTLSCertPath returns the path for the PgFox client-facing TLS cert.
// CN=Hostname — must match what clients use to connect to PgFox.
func (p *Server) pgfoxTLSCertPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "pgfox.crt")
}

func (p *Server) pgfoxTLSKeyPath() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "pgfox.key")
}

func (p *Server) certsDir() string {
	return filepath.Join(p.Config.Server.PgFoxDir, "certs")
}

func (p *Server) pgfoxCertPath() string {
	return filepath.Join(p.certsDir(), p.Config.Server.PgFoxRole+".crt")
}

func (p *Server) pgfoxKeyPath() string {
	return filepath.Join(p.certsDir(), p.Config.Server.PgFoxRole+".key")
}

func (p *Server) userCertPath(username string) string {
	return filepath.Join(p.certsDir(), username+".crt")
}

func (p *Server) userKeyPath(username string) string {
	return filepath.Join(p.certsDir(), username+".key")
}

// =============================================================================
// Bootstrap certificate generation
// =============================================================================

// ensureBootstrapCerts checks for and generates all bootstrap certificates
// that must exist before PgFox can start. Called once during Start().
//
// Files generated (only if missing or invalid):
//
//	{pgfox_dir}/ca.crt + ca.key       — CA (self-signed, 10 years)
//	{pgfox_dir}/server.crt + .key     — PostgreSQL server cert (CN+SAN=first target host)
//	{pgfox_dir}/pgfox.crt + .key      — PgFox client-facing TLS cert (CN+SAN=Hostname)
//
// Both server.crt and pgfox.crt include proper SANs required by Go 1.15+.
// Operators copy server.crt/key to $PGDATA and configure postgresql.conf.
func (p *Server) ensureBootstrapCerts() error {
	if err := os.MkdirAll(p.Config.Server.PgFoxDir, 0700); err != nil {
		return fmt.Errorf("failed to create pgfox_dir %s: %w", p.Config.Server.PgFoxDir, err)
	}

	if err := p.ensureCA(); err != nil {
		return fmt.Errorf("CA setup failed: %w", err)
	}

	caCert, caKey, err := p.loadCA()
	if err != nil {
		return fmt.Errorf("failed to load CA for bootstrap: %w", err)
	}

	// PostgreSQL server cert — CN+SAN = first target host
	serverHost := "localhost"
	if len(p.Config.Targets) > 0 {
		serverHost = p.Config.Targets[0].Host
	}
	if err := p.ensureSignedCert(
		p.serverCertPath(), p.serverKeyPath(),
		serverHost, "PostgreSQL Server", caCert, caKey,
	); err != nil {
		return fmt.Errorf("server cert setup failed: %w", err)
	}

	// PgFox client-facing TLS cert — CN+SAN = Hostname
	if err := p.ensureSignedCert(
		p.pgfoxTLSCertPath(), p.pgfoxTLSKeyPath(),
		p.Config.Server.Hostname, "PgFox Server", caCert, caKey,
	); err != nil {
		return fmt.Errorf("pgfox TLS cert setup failed: %w", err)
	}

	p.Logger.Info("Bootstrap certificates ready",
		"ca", p.caCertPath(),
		"server_cert", p.serverCertPath(),
		"pgfox_cert", p.pgfoxTLSCertPath())

	p.Logger.Info("PostgreSQL server setup — copy these files to $PGDATA:",
		"ssl_cert_file", p.serverCertPath(),
		"ssl_key_file", p.serverKeyPath(),
		"ssl_ca_file", p.caCertPath())

	return nil
}

// ensureCA generates a self-signed CA cert and key if either is missing.
func (p *Server) ensureCA() error {
	caCertExists := fileExists(p.caCertPath())
	caKeyExists := fileExists(p.caKeyPath())

	if caCertExists && caKeyExists {
		p.Logger.Debug("CA already exists", "path", p.caCertPath())
		return nil
	}
	if caCertExists != caKeyExists {
		p.Logger.Warn("CA cert and key inconsistent — regenerating both",
			"cert_exists", caCertExists, "key_exists", caKeyExists)
	}

	p.Logger.Info("Generating CA certificate", "path", p.caCertPath())

	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate CA key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate CA serial: %w", err)
	}

	cfg := p.Config.Server.Certs
	now := time.Now()

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         "PgFox CA",
			Organization:       []string{cfg.Organization},
			OrganizationalUnit: []string{"Certificate Authority"},
			Country:            []string{cfg.Country},
		},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create CA certificate: %w", err)
	}

	if err := writeCertFile(p.caCertPath(), certDER); err != nil {
		return fmt.Errorf("failed to write CA cert: %w", err)
	}
	if err := writeKeyFile(p.caKeyPath(), caKey); err != nil {
		return fmt.Errorf("failed to write CA key: %w", err)
	}

	p.Logger.Info("CA certificate generated",
		"cert", p.caCertPath(),
		"expires", template.NotAfter.Format(time.RFC3339))

	return nil
}

// ensureSignedCert generates a certificate signed by the CA if the cert file
// does not already exist or is not valid for the current CA.
// cn is used as both the CommonName and the SAN (IP or DNS depending on format).
func (p *Server) ensureSignedCert(certPath, keyPath, cn, ou string,
	caCert *x509.Certificate, caKey *rsa.PrivateKey) error {

	if p.isCertValidForCA(certPath, true) {
		p.Logger.Debug("Certificate already valid", "path", certPath, "cn", cn)
		return nil
	}

	p.Logger.Info("Generating certificate", "path", certPath, "cn", cn)

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("failed to generate key for %s: %w", cn, err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("failed to generate serial for %s: %w", cn, err)
	}

	cfg := p.Config.Server.Certs
	now := time.Now()

	// Go 1.15+ requires SANs for server certificate hostname verification.
	// Add cn as an IP SAN if it parses as an IP, otherwise as a DNS SAN.
	var ipSANs []net.IP
	var dnsSANs []string
	if ip := net.ParseIP(cn); ip != nil {
		ipSANs = []net.IP{ip}
	} else {
		dnsSANs = []string{cn}
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:         cn,
			Organization:       []string{cfg.Organization},
			OrganizationalUnit: []string{ou},
			Country:            []string{cfg.Country},
		},
		NotBefore:             now.Add(-1 * time.Minute),
		NotAfter:              now.Add(cfg.TTL),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		IPAddresses:           ipSANs,
		DNSNames:              dnsSANs,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("failed to create certificate for %s: %w", cn, err)
	}

	if err := writeCertFile(certPath, certDER); err != nil {
		return fmt.Errorf("failed to write cert %s: %w", certPath, err)
	}
	if err := writeKeyFile(keyPath, privateKey); err != nil {
		return fmt.Errorf("failed to write key %s: %w", keyPath, err)
	}

	p.Logger.Info("Certificate generated",
		"path", certPath,
		"cn", cn,
		"expires", template.NotAfter.Format(time.RFC3339))

	return nil
}

// writeCertFile writes a DER-encoded certificate to a PEM file (mode 0644).
func writeCertFile(path string, certDER []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

// writeKeyFile writes an RSA private key to a PEM file (mode 0600).
func writeKeyFile(path string, key *rsa.PrivateKey) error {
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

// fileExists returns true if the file exists and is readable.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
