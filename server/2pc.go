package main

// Implement Prepare RPC
// func (s *Server) Prepare(ctx context.Context, req *pb.ClientPrepare) (*pb.ClientPrepareResponse, error) {
// 	// txn := req.Transaction
// 	// Check if the transaction can be committed (e.g., sufficient balance)
// 	// canCommit := s.canCommitTransaction(txn)
// 	// if canCommit {
// 	// 	// Record the prepared state
// 	// 	s.transactionLog[txn.Id] = &TransactionState{txn: txn, prepared: true}
// 	// }
// 	// return &pb.ClientPrepareResponse{CanCommit: canCommit}, nil
// 	return &pb.ClientPrepareResponse{CanCommit: true}, nil
// }

// Implement Commit RPC
// func (s *Server) Commit(ctx context.Context, req *pb.ClientCommit) (*emptypb.Empty, error) {
// 	txnID := req.TransactionId
// 	state, exists := s.transactionLog[txnID]
// 	if exists && state.prepared {
// 		// Commit the transaction
// 		// s.commitTransaction(state.txn)
// 		delete(s.transactionLog, txnID)
// 	}
// 	return &emptypb.Empty{}, nil
// }

// Implement Abort RPC
// func (s *Server) Abort(ctx context.Context, req *pb.ClientAbort) (*emptypb.Empty, error) {
// 	txnID := req.TransactionId
// 	// Remove the transaction from the log
// 	delete(s.transactionLog, txnID)
// 	return &emptypb.Empty{}, nil
// }
