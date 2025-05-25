package aggregator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agglayer/aggkit/agglayer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockAgglayerClient struct {
	mock.Mock
}

func (m *MockAgglayerClient) SendCertificate(ctx context.Context, cert *types.Certificate) (common.Hash, error) {
	args := m.Called(ctx, cert)
	return args.Get(0).(common.Hash), args.Error(1)
}

func (m *MockAgglayerClient) GetCertificateHeader(ctx context.Context, certID common.Hash) (*types.CertificateHeader, error) {
	args := m.Called(ctx, certID)
	return args.Get(0).(*types.CertificateHeader), args.Error(1)
}

func TestSendCertificateAndWait(t *testing.T) {
	cert := &types.Certificate{
		NetworkID:        1,
		Height:           10,
		NewLocalExitRoot: common.HexToHash("0xabc123"),
	}

	certID := common.HexToHash("0xdeadbeef")

	t.Run("success - certificate settled", func(t *testing.T) {
		mockClient := new(MockAgglayerClient)

		mockClient.On("SendCertificate", mock.Anything, cert).Return(certID, nil)
		mockClient.On("GetCertificateHeader", mock.Anything, certID).Return(&types.CertificateHeader{
			Status: types.Settled,
		}, nil)

		success, err := sendCertificateAndWait(context.Background(), mockClient, cert, 2*time.Second)

		assert.NoError(t, err)
		assert.True(t, success)
		mockClient.AssertExpectations(t)
	})

	t.Run("failure - certificate rejected", func(t *testing.T) {
		mockClient := new(MockAgglayerClient)

		mockClient.On("SendCertificate", mock.Anything, cert).Return(certID, nil)
		mockClient.On("GetCertificateHeader", mock.Anything, certID).Return(&types.CertificateHeader{
			Status: types.InError,
			Error:  errors.New("agg rejected"),
		}, nil)

		success, err := sendCertificateAndWait(context.Background(), mockClient, cert, 2*time.Second)

		assert.Error(t, err)
		assert.False(t, success)
		mockClient.AssertExpectations(t)
	})

	t.Run("failure - sendCertificate fails", func(t *testing.T) {
		mockClient := new(MockAgglayerClient)

		mockClient.On("SendCertificate", mock.Anything, cert).Return(common.Hash{}, errors.New("network failure"))

		success, err := sendCertificateAndWait(context.Background(), mockClient, cert, 2*time.Second)

		assert.Error(t, err)
		assert.False(t, success)
		mockClient.AssertExpectations(t)
	})

	t.Run("timeout - certificate never settles", func(t *testing.T) {
		mockClient := new(MockAgglayerClient)

		mockClient.On("SendCertificate", mock.Anything, cert).Return(certID, nil)
		mockClient.On("GetCertificateHeader", mock.Anything, certID).Return(&types.CertificateHeader{
			Status: types.Pending,
		}, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		success, err := sendCertificateAndWait(ctx, mockClient, cert, 1*time.Second)

		assert.Error(t, err)
		assert.False(t, success)
		mockClient.AssertExpectations(t)
	})
}
