import 'dart:async';
import 'dart:convert';
import 'dart:developer';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:bb_mobile/core/errors/bull_exception.dart';
import 'package:bb_mobile/core/payjoin/data/models/payjoin_input_pair_model.dart';
import 'package:bb_mobile/core/payjoin/data/models/payjoin_model.dart';
import 'package:bb_mobile/core/utils/constants.dart';
import 'package:bb_mobile/core/utils/logger.dart' as logger;
import 'package:bb_mobile/core/utils/bitcoin_tx.dart';
import 'package:dio/dio.dart';
import 'package:payjoin/payjoin.dart' as pj;

class PdkPayjoinDatasource {
  final String _payjoinDirectoryUrl;
  final Dio _dio;
  final StreamController<PayjoinReceiverModel> _payjoinRequestedController;
  final StreamController<PayjoinSenderModel> _proposalSentController;
  final StreamController<PayjoinModel> _expiredController;

  // Background processing
  Isolate? _receiversIsolate;
  Isolate? _sendersIsolate;
  SendPort? _receiversIsolatePort;
  SendPort? _sendersIsolatePort;
  final Completer _receiversIsolateReady;
  final Completer _sendersIsolateReady;

  PdkPayjoinDatasource({
    String payjoinDirectoryUrl = PayjoinConstants.directoryUrl,
    required Dio dio,
  }) : _payjoinDirectoryUrl = payjoinDirectoryUrl,
       _dio = dio,
       _payjoinRequestedController = StreamController.broadcast(),
       _proposalSentController = StreamController.broadcast(),
       _expiredController = StreamController.broadcast(),
       _receiversIsolateReady = Completer(),
       _sendersIsolateReady = Completer();

  Stream<PayjoinReceiverModel> get requestsForReceivers =>
      _payjoinRequestedController.stream;

  Stream<PayjoinSenderModel> get proposalsForSenders =>
      _proposalSentController.stream;

  Stream<PayjoinModel> get expiredPayjoins => _expiredController.stream;

  /// Fetch OHTTP keys by making an HTTP GET to the payjoin directory,
  /// then decoding the response bytes with OhttpKeys.decode().
  ///
  /// The old `fetchOhttpKeys()` top-level function no longer exists in the
  /// public API; it was a test-only utility.
  Future<(pj.OhttpKeys?, String?)> fetchOhttpKeyAndRelay({
    required String payjoinDirectory,
  }) async {
    String? ohttpRelay;
    pj.OhttpKeys? ohttpKeys;
    final dio = Dio();
    for (final ohttpRelayUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        // Fetch OHTTP keys from the directory via the relay
        final response = await dio.get(
          '$payjoinDirectory/ohttp-keys',
          options: Options(responseType: ResponseType.bytes),
        );
        final bytes = response.data as List<int>;
        ohttpKeys = pj.OhttpKeys.decode(bytes: Uint8List.fromList(bytes));
        ohttpRelay = ohttpRelayUrl;
        break;
      } catch (e) {
        continue;
      }
    }
    return (ohttpKeys, ohttpRelay);
  }

  Future<PayjoinReceiverModel> createReceiver({
    required String walletId,
    required String address,
    required bool isTestnet,
    required BigInt maxFeeRateSatPerVb,
    required int expireAfterSec,
  }) async {
    try {
      final (ohttpKeys, ohttpRelay) = await fetchOhttpKeyAndRelay(
        payjoinDirectory: _payjoinDirectoryUrl,
      );

      if (ohttpRelay == null || ohttpKeys == null) {
        throw Exception('All OHTTP relays failed');
      }

      final persister = ReceiverPersisterAdapter();

      // ReceiverBuilder takes a string address directly (no Address wrapper)
      final initialReceiveTransition =
          pj.ReceiverBuilder(
            address: address,
            directory: _payjoinDirectoryUrl,
            ohttpKeys: ohttpKeys,
          )
          .withExpiration(expiration: expireAfterSec)
          .build()
          .save(persister: persister);

      final initialized = initialReceiveTransition;

      // Create and store the model to keep track of the payjoin session
      // TODO: Get proper ID from the session and ensure it's queryable later on
      final sessionId = DateTime.now().millisecondsSinceEpoch.toString();
      final model =
          PayjoinModel.receiver(
                id: sessionId,
                address: address,
                isTestnet: isTestnet,
                receiver: persister.toJson(),
                walletId: walletId,
                pjUri: initialized.pjUri().asString(),
                maxFeeRateSatPerVb: maxFeeRateSatPerVb,
                createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
                expireAfterSec: expireAfterSec,
              )
              as PayjoinReceiverModel;

      // Start listening for a payjoin request from the sender in an isolate
      await startListeningForRequest(model);

      return model;
    } catch (e) {
      throw ReceiveCreationException(e.toString());
    }
  }

  Future<PayjoinSenderModel> createSender({
    required String walletId,
    required bool isTestnet,
    required String bip21,
    required String originalPsbt,
    required int amountSat,
    required double networkFeesSatPerVb,
    int? expireAfterSec,
  }) async {
    final expirySec = expireAfterSec ?? PayjoinConstants.defaultExpireAfterSec;
    final uri = pj.Uri.parse(uri: bip21);

    pj.PjUri pjUri;
    try {
      pjUri = uri.checkPjSupported();
    } catch (e) {
      throw NoValidPayjoinBip21Exception(e.toString());
    }

    final minFeeRateSatPerKwu = (networkFeesSatPerVb * 250).toInt();
    final persister = SenderPersisterAdapter();

    final initialSendTransition = pj.SenderBuilder(
      psbt: originalPsbt,
      uri: pjUri,
    ).buildRecommended(minFeeRate: minFeeRateSatPerKwu);
    initialSendTransition.save(persister: persister);

    // Create and store the model with the data needed to keep track of the
    //  payjoin session
    final model =
        PayjoinModel.sender(
              uri: uri.asString(),
              isTestnet: isTestnet,
              sender: persister.toJson(),
              walletId: walletId,
              originalPsbt: originalPsbt,
              originalTxId: await _getTxIdFromPsbt(originalPsbt),
              amountSat: amountSat,
              createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
              expireAfterSec: expirySec,
            )
            as PayjoinSenderModel;

    // Start listening for a payjoin proposal from the receiver in an isolate
    await startListeningForProposal(model);

    return model;
  }

  Future<PayjoinReceiverModel> proposePayjoin({
    required PayjoinReceiverModel receiverModel,
    required FutureOr<bool> Function(Uint8List) hasOwnedInputs,
    required FutureOr<bool> Function(Uint8List) hasReceiverOutput,
    required List<PayjoinInputPairModel> inputPairs,
    required FutureOr<String> Function(String) processPsbt,
  }) async {
    final persister = ReceiverPersisterAdapter.fromJson(receiverModel.receiver);

    final replayResult = pj.replayReceiverEventLog(persister: persister);
    final receiveSession = replayResult.state();

    pj.UncheckedOriginalPayload request;

    if (receiveSession is pj.InitializedReceiveSession) {
      final result = await getRequest(
        receiver: receiveSession.inner,
        dio: _dio,
        persister: persister,
      );
      if (result == null) {
        throw Exception('No request found');
      }
      request = result;
    } else if (receiveSession is pj.UncheckedOriginalPayloadReceiveSession) {
      request = receiveSession.inner;
    } else {
      throw Exception(
        'TODO handle session state: ${receiveSession.runtimeType}',
      );
    }

    final interactiveReceiver = request.assumeInteractiveReceiver().save(
      persister: persister,
    );
    final inputsNotOwned = interactiveReceiver
        .checkInputsNotOwned(isOwned: IsScriptOwnedCallback(hasOwnedInputs))
        .save(persister: persister);
    final inputsNotSeen = inputsNotOwned
        .checkNoInputsSeenBefore(
          // Assume the wallet has not seen the inputs since it is an interactive wallet
          isKnown: IsOutputKnownCallback((_) => false),
        )
        .save(persister: persister);
    final receiverOutputs = inputsNotSeen
        .identifyReceiverOutputs(isReceiverOutput: IsScriptOwnedCallback(hasReceiverOutput))
        .save(persister: persister);
    final committedOutputs = receiverOutputs.commitOutputs().save(persister: persister);

    final candidateInputs =
        inputPairs
            .map(
              (input) => pj.InputPair(
                txin: pj.PlainTxIn(
                  previousOutput: pj.PlainOutPoint(
                    txid: input.txId,
                    vout: input.vout,
                  ),
                  scriptSig: Uint8List.fromList(input.scriptSigRawOutputScript),
                  sequence: input.sequence,
                  witness: input.witness,
                ),
                psbtin: pj.PlainPsbtInput(
                  witnessUtxo: input.value != null
                      ? pj.PlainTxOut(
                          valueSat: input.value!.toInt(),
                          scriptPubkey: input.scriptPubkey,
                        )
                      : null,
                  redeemScript: input.redeemScriptRawOutputScript.isEmpty
                      ? null
                      : Uint8List.fromList(input.redeemScriptRawOutputScript),
                  witnessScript: input.witnessScriptRawOutputScript.isEmpty
                      ? null
                      : Uint8List.fromList(input.witnessScriptRawOutputScript),
                ),
                expectedWeight: null,
              ),
            )
            .toList();

    // Try to select a privacy preserving input pair, else just stick with the
    //  first possible input pair.
    pj.InputPair inputPair = candidateInputs.first;
    try {
      inputPair = committedOutputs.tryPreservingPrivacy(
        candidateInputs: candidateInputs,
      );
    } catch (e) {
      logger.log.severe(
        message: 'Failed to preserve privacy: $e. Using first input pair.',
        error: e,
        trace: StackTrace.current,
      );
    }

    final inputsContributed = committedOutputs.contributeInputs(
      replacementInputs: [inputPair],
    );
    final inputsCommitted = inputsContributed.commitInputs().save(persister: persister);
    final feesApplied = inputsCommitted
        .applyFeeRange(
          minFeeRateSatPerVb: null,
          maxEffectiveFeeRateSatPerVb: receiverModel.maxFeeRateSatPerVb.toInt(),
        )
        .save(persister: persister);
    final proposal = feesApplied
        .finalizeProposal(processPsbt: ProcessPsbtCallback(processPsbt))
        .save(persister: persister);

    // Now that the request is processed and the proposal is ready, send it to
    //  the sender through the payjoin directory
    await _sendPayjoinProposal(proposal, persister);

    // Update the model with the proposal psbt so it can be known a proposal has
    //  been sent
    final proposalPsbt = proposal.psbt();

    final updatedModel = receiverModel.copyWith(
      receiver: persister.toJson(),
      proposalPsbt: proposalPsbt,
      txId: await _getTxIdFromPsbt(proposalPsbt),
    );

    logger.log.info(
      'Payjoin request processed and proposal sent: $proposalPsbt',
    );

    return updatedModel;
  }

  Future<void> startListeningForRequest(PayjoinReceiverModel payjoin) async {
    if (_receiversIsolate == null) {
      // Start the isolate if it is not running yet
      await _spawnReceiversIsolate();
    }
    await _receiversIsolateReady.future;
    _receiversIsolatePort?.send(payjoin.toJson());
  }

  Future<void> startListeningForProposal(PayjoinSenderModel payjoin) async {
    if (_sendersIsolate == null) {
      // Start the isolate if it is not running yet
      await _spawnSendersIsolate();
    }
    await _sendersIsolateReady.future;
    _sendersIsolatePort?.send(payjoin.toJson());
  }

  /// Starts the isolate to listen for payjoin requests.
  Future<void> _spawnReceiversIsolate() async {
    // Receive isolate
    final receivePort = ReceivePort();

    // Listen to messages from the receive isolate
    receivePort.listen((message) {
      if (message is SendPort) {
        _receiversIsolatePort = message;
        _receiversIsolateReady.complete();
      } else if (message is Map<String, dynamic>) {
        logger.log.info(
          'Received message of found payjoin request in main isolate: $message',
        );
        final model = PayjoinReceiverModel.fromJson(message);

        // Send the updated payjoin model to the higher repository layers so it
        //  can be stored locally and processed further
        if (model.isExpired) {
          _expiredController.add(model);
        } else {
          // If not expired, it means a request was received
          _payjoinRequestedController.add(model);
        }
      }
    });

    logger.log.info('Spawning receivers isolate');
    // Spawn the isolate
    _receiversIsolate = await Isolate.spawn(
      _receiversIsolateEntryPoint,
      receivePort.sendPort,
    );
  }

  /// Starts the isolate to request and listen for payjoin proposals.
  Future<void> _spawnSendersIsolate() async {
    // Senders isolate
    final receivePort = ReceivePort();

    // Listen for messages from the senders isolate
    receivePort.listen((message) {
      if (message is SendPort) {
        _sendersIsolatePort = message;
        _sendersIsolateReady.complete();
      } else if (message is Map<String, dynamic>) {
        final model = PayjoinSenderModel.fromJson(message);

        // Send the updated payjoin model to the higher repository layers for
        //  processing and notification to the user
        if (model.isExpired) {
          _expiredController.add(model);
        } else {
          // If not expired, it means a proposal was received
          _proposalSentController.add(model);
        }
      }
    });

    logger.log.info('Spawning senders isolate');
    _sendersIsolate = await Isolate.spawn(
      _sendersIsolateEntryPoint,
      receivePort.sendPort,
    );
  }

  static Future<void> _receiversIsolateEntryPoint(SendPort sendPort) async {
    log('[Receivers Isolate] Started _receiversIsolateEntryPoint');

    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);
    final dio = Dio();
    final requests = <String, Future<void>>{};

    // Listen for and register new receivers sent from the main isolate
    receivePort.listen((data) {
      log('[Receivers Isolate] Received data in receivers isolate: $data');
      final receiverModel = PayjoinReceiverModel.fromJson(
        data as Map<String, dynamic>,
      );
      final persister = ReceiverPersisterAdapter.fromJson(
        receiverModel.receiver,
      );
      final replayResult = pj.replayReceiverEventLog(persister: persister);
      final receiveSession = replayResult.state();
      if (receiveSession is! pj.InitializedReceiveSession) {
        log(
          '[Receivers Isolate] Expected InitializedReceiveSession but got ${receiveSession.runtimeType}',
        );
        return;
      }
      final receiver = receiveSession.inner;

      // Start checking for a payjoin request from the sender periodically
      const interval = Duration(
        seconds: PayjoinConstants.directoryPollingInterval,
      );
      Timer.periodic(interval, (Timer timer) async {
        log('[Receivers Isolate] Checking for request in receivers isolate');
        try {
          final request = await getRequest(
            receiver: receiver,
            dio: dio,
            persister: persister,
          );
          if (request != null) {
            requests.putIfAbsent(receiver.pjUri().asString(), () async {
              log('[Receivers Isolate] Request found in receivers isolate');
              // The original tx bytes are needed in the main isolate for
              //  further processing so extract them here and pass them through
              //  the model
              final extractable = request.assumeInteractiveReceiver().save(
                persister: persister,
              );
              final originalTxBytes =
                  extractable.extractTxToScheduleBroadcast();
              final originalTxId =
                  await _getTxIdFromTransactionBytes(originalTxBytes);
              final amountSat =
                  await _getAmountReceivedFromTransactionBytes(
                    originalTxBytes,
                    address: receiverModel.address,
                    isTestnet: receiverModel.isTestnet,
                  );
              log(
                '[Receivers Isolate] Request original Tx ID: $originalTxId and amount: $amountSat',
              );
              final updatedModel = receiverModel.copyWith(
                receiver: persister.toJson(),
                originalTxBytes: originalTxBytes,
                originalTxId: originalTxId,
                amountSat: amountSat,
              );

              // Notify the main isolate so it can be processed further
              sendPort.send(updatedModel.toJson());

              // Cancel the timer since the request has been received
              log('[Receivers Isolate] cancelling timer in receivers isolate');
              timer.cancel();
              log('[Receivers Isolate] timer cancelled in receivers isolate');
            });
          } else {
            log(
              '[Receivers Isolate] No valid request found in receivers isolate',
            );
          }
        } catch (e) {
          log('[Receivers Isolate] periodic timer get request exception: $e');
          if (e is PayjoinExpiredException) {
            // If the request returns an expiry error, mark the receiver as
            //  expired and notify the main isolate so it stops polling
            final updatedModel = receiverModel.copyWith(isExpired: true);
            sendPort.send(updatedModel.toJson());
            timer.cancel();
          }
        }
      });
    });
  }

  static Future<void> _sendersIsolateEntryPoint(SendPort sendPort) async {
    log('[Senders Isolate] Started _sendersIsolateEntryPoint');

    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);

    final dio = Dio();
    // Listen for and register new receivers sent from the main isolate
    receivePort.listen((data) async {
      try {
        log('[Senders Isolate] Received data in senders isolate: $data');
        final senderModel = PayjoinSenderModel.fromJson(
          data as Map<String, dynamic>,
        );
        final persister = SenderPersisterAdapter.fromJson(senderModel.sender);

        final replayResult = pj.replaySenderEventLog(persister: persister);
        final sendSession = replayResult.state();

        if (sendSession is! pj.WithReplyKeySendSession) {
          log(
            '[Senders Isolate] Expected WithReplyKeySendSession but got ${sendSession.runtimeType}',
          );
          return;
        }

        final sender = sendSession.inner;
        log('[Senders Isolate] Requesting payjoin...');
        final pollingForProposal = await PdkPayjoinDatasource.request(
          sender: sender,
          dio: dio,
          persister: persister,
        );
        log('[Senders Isolate] Payjoin requested.');

        // Periodically check for a proposal from the receiver
        Timer.periodic(
          const Duration(seconds: PayjoinConstants.directoryPollingInterval),
          (Timer timer) async {
            log('[Senders Isolate]Checking for proposal in senders isolate');
            try {
              final proposalPsbtBase64 =
                  await PdkPayjoinDatasource.getProposalPsbt(
                pollingForProposal: pollingForProposal,
                dio: dio,
                persister: persister,
              );

              if (proposalPsbtBase64 != null) {
                log('[Senders Isolate] Proposal found in senders isolate');
                final txId = await _getTxIdFromPsbt(proposalPsbtBase64);
                // The proposal psbt is needed in the main isolate for
                //  further processing so send it through the model as well as
                //  its txId.
                final updatedModel = senderModel.copyWith(
                  proposalPsbt: proposalPsbtBase64,
                  txId: txId,
                );

                // Notify the main isolate so the payjoin can be processed further
                sendPort.send(updatedModel.toJson());

                // Cancel the timer
                timer.cancel();
              }
            } catch (e) {
              log('[Senders Isolate] periodic timer exception: $e');
              if (e is PayjoinExpiredException) {
                // If the request returns an expiry error, mark the receiver as
                //  expired and notify the main isolate so it stops polling
                final updatedModel = senderModel.copyWith(isExpired: true);
                sendPort.send(updatedModel.toJson());
                timer.cancel();
              }
            }
          },
        );
      } catch (e) {
        log('[Senders Isolate] Error in listener: $e');
        // Optionally notify the main isolate of the failure
      }
    });
  }

  static Future<pj.UncheckedOriginalPayload?> getRequest({
    required pj.Initialized receiver,
    required Dio dio,
    required ReceiverPersisterAdapter persister,
  }) async {
    // The use of ffiError here is a hack, we should change it once payjoin-flutter
    //  exposes different exceptions for specific errors
    Object? ffiError;
    try {
      pj.RequestResponse? request;
      for (final ohttpRelay in PayjoinConstants.ohttpRelayUrls) {
        try {
          request = receiver.createPollRequest(ohttpRelay: ohttpRelay);
          ffiError = null;
          log('receiver extractReq success');
          break;
        } catch (e) {
          log('receiver extractReq exception: $e');
          ffiError = e;
          continue;
        }
      }

      if (request == null) {
        if (ffiError != null) {
          throw ffiError;
        }
        throw PayjoinNotFoundException('No payjoin request found');
      }

      log('request != null');
      final (req, context) = (request.request, request.clientResponse);
      final ohttpResponse = await dio.post(
        req.url,
        data: req.body,
        options: Options(
          headers: {'Content-Type': req.contentType},
          responseType: ResponseType.bytes,
        ),
      );
      log('processing request...');
      final proposal = receiver
          .processResponse(
            body: ohttpResponse.data as Uint8List,
            ctx: context,
          )
          .save(persister: persister);
      log('request processed');
      if (proposal is pj.ProgressInitializedTransitionOutcome) {
        return proposal.inner;
      } else {
        return null;
      }
    } catch (e) {
      log('getRequest exception: $e');
      if (e == ffiError) {
        // TODO: Check for the correct error.
        //  We just assume the error is an expired error for now.
        throw PayjoinExpiredException(
          'Payjoin receiver expired',
        );
      }
      return null;
    }
  }

  Future<void> _sendPayjoinProposal(
    pj.PayjoinProposal proposal,
    ReceiverPersisterAdapter persister,
  ) async {
    pj.RequestResponse? request;
    for (final ohttpRelayUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        request = proposal.createPostRequest(ohttpRelay: ohttpRelayUrl);
        break;
      } catch (e) {
        log('proposal extractReq exception: $e with relay $ohttpRelayUrl');
        continue;
      }
    }
    if (request == null) {
      throw PayjoinNotFoundException('No payjoin proposal found');
    }

    final (req, ohttpCtx) = (request.request, request.clientResponse);
    final res = await _dio.post(
      req.url,
      data: req.body,
      options: Options(
        headers: {'Content-Type': req.contentType},
        responseType: ResponseType.bytes,
      ),
    );
    // Save the transition to persist state (e.g. move to Monitor or Closed)
    proposal.processResponse(
      body: res.data as Uint8List,
      ohttpContext: ohttpCtx,
    ).save(persister: persister);
  }

  static Future<pj.PollingForProposal> request({
    required pj.WithReplyKey sender,
    required Dio dio,
    required SenderPersisterAdapter persister,
  }) async {
    pj.RequestOhttpContext? result;

    for (final ohttpProxyUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        log(
          '[Senders Isolate] Extracting V2 request from sender with relay: $ohttpProxyUrl',
        );
        result = sender.createV2PostRequest(ohttpRelay: ohttpProxyUrl);
        break;
      } catch (e) {
        log('[Senders Isolate] request error: $e');
        log('[Senders Isolate] Continuing to next OHTTP relay');
        continue;
      }
    }

    if (result == null) {
      log('[Senders Isolate] All OHTTP relays failed');
      throw Exception('All OHTTP relays failed');
    }

    final (req, context) = (result.request, result.ohttpCtx);

    log('[Senders Isolate] Sending V2 request to ${req.url}');
    final res = await dio.post(
      req.url,
      data: req.body,
      options: Options(
        headers: {'Content-Type': req.contentType},
        responseType: ResponseType.bytes,
      ),
    );
    log('[Senders Isolate] Received response from ${req.url}');

    final pollingForProposal = sender
        .processResponse(
          response: res.data as Uint8List,
          postCtx: context,
        )
        .save(persister: persister);

    log(
      '[Senders Isolate] Processed response for V2 request: $pollingForProposal ',
    );

    return pollingForProposal;
  }

  /// Returns the payjoin proposal PSBT as a base64 string, or null if not
  /// yet available (stasis/polling).
  static Future<String?> getProposalPsbt({
    required pj.PollingForProposal pollingForProposal,
    required Dio dio,
    required SenderPersisterAdapter persister,
  }) async {
    // The use of ffiError here is a hack, we should change it once payjoin-flutter
    //  exposes different exceptions for specific errors
    Object? ffiError;
    try {
      pj.RequestOhttpContext? result;
      for (final ohttpRelay in PayjoinConstants.ohttpRelayUrls) {
        try {
          result = pollingForProposal.createPollRequest(ohttpRelay: ohttpRelay);
          ffiError = null;
          log(
            'context extract request success: $result with relay $ohttpRelay',
          );
          break;
        } catch (e) {
          log('context extract request exception: $e with relay $ohttpRelay');
          ffiError = e;
          continue;
        }
      }

      if (result == null) {
        if (ffiError != null) {
          throw ffiError;
        }
        throw Exception('All OHTTP relays failed');
      }

      final (req, reqCtx) = (result.request, result.ohttpCtx);

      final res = await dio.post(
        req.url,
        data: req.body,
        options: Options(
          headers: {'Content-Type': req.contentType},
          responseType: ResponseType.bytes,
        ),
      );

      final proposalResult = pollingForProposal
          .processResponse(
            response: res.data as Uint8List,
            ohttpCtx: reqCtx,
          )
          .save(persister: persister);

      if (proposalResult is pj.ProgressPollingForProposalTransitionOutcome) {
        // The psbt is now a base64 String, not a Psbt object
        return proposalResult.psbtBase64;
      } else {
        return null;
      }
    } catch (e) {
      log('getProposalPsbt exception: $e');
      if (e == ffiError) {
        // TODO: Check for the correct error.
        //  We just assume the error is an expired error for now.
        throw PayjoinExpiredException('Payjoin sender expired');
      }
      return null;
    }
  }

  /// Helper to get txid from PSBT base64 string using BitcoinTx.
  static Future<String> _getTxIdFromPsbt(String psbtBase64) async {
    final tx = await BitcoinTx.fromPsbt(psbtBase64);
    return tx.txid;
  }

  /// Helper to get txid from raw transaction bytes using BitcoinTx.
  static Future<String> _getTxIdFromTransactionBytes(
    Uint8List txBytes,
  ) async {
    final tx = await BitcoinTx.fromBytes(txBytes);
    return tx.txid;
  }

  /// Helper to get amount received from raw transaction bytes using BitcoinTx.
  static Future<int> _getAmountReceivedFromTransactionBytes(
    Uint8List txBytes, {
    required String address,
    required bool isTestnet,
  }) async {
    final tx = await BitcoinTx.fromBytes(txBytes);
    return tx.getAmountReceived(address: address, isTestnet: isTestnet);
  }
}

/// Temporary adapters for payjoin receiver and sender session persistence.
///
/// This adapter bridges the gap between the old payjoin_flutter API (which required
/// full object serialization) and the new payjoin_dart API (which uses session events).
/// It implements JsonReceiverSessionPersister by storing session events in memory.
///
/// Events are also persisted as JSON in the existing PayjoinModel.receiver field
/// and database. This is a hack but allows upgrading the payjoin dependency without
/// any database schema changes.
class ReceiverPersisterAdapter implements pj.JsonReceiverSessionPersister {
  final List<String> _events = [];

  @override
  void save(String event) {
    _events.add(event);
  }

  @override
  List<String> load() {
    return _events;
  }

  @override
  void close() {
    // No-op for now
  }

  String toJson() {
    return jsonEncode(_events);
  }

  static ReceiverPersisterAdapter fromJson(String json) {
    final adapter = ReceiverPersisterAdapter();
    final events = jsonDecode(json) as List<dynamic>;
    adapter._events.addAll(events.cast<String>());
    return adapter;
  }
}

class SenderPersisterAdapter implements pj.JsonSenderSessionPersister {
  final List<String> _events = [];

  @override
  void save(String event) {
    _events.add(event);
  }

  @override
  List<String> load() {
    return _events;
  }

  @override
  void close() {
    // No-op for now
  }

  String toJson() {
    return jsonEncode(_events);
  }

  static SenderPersisterAdapter fromJson(String json) {
    final adapter = SenderPersisterAdapter();
    final events = jsonDecode(json) as List<dynamic>;
    adapter._events.addAll(events.cast<String>());
    return adapter;
  }
}

// These callback wrappers convert async functions to sync as required by
// the uniffi-generated callback traits. The callbacks return plain values
// (bool or String), matching the Dart uniffi-generated trait signatures.
class IsScriptOwnedCallback implements pj.IsScriptOwned {
  final FutureOr<bool> Function(Uint8List) _callback;

  IsScriptOwnedCallback(this._callback);

  @override
  bool callback(Uint8List script) {
    final result = _callback(script);
    if (result is Future<bool>) {
      // NOTE: This busy-wait pattern is inherited from the original code.
      // It works because these callbacks run within the FFI boundary synchronously.
      // A proper fix would use async callbacks if/when the FFI supports them.
      final completer = Completer<bool>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as bool;
    }
    return result;
  }
}

class IsOutputKnownCallback implements pj.IsOutputKnown {
  final FutureOr<bool> Function(dynamic) _callback;

  IsOutputKnownCallback(this._callback);

  @override
  bool callback(pj.PlainOutPoint outpoint) {
    final result = _callback(outpoint);
    if (result is Future<bool>) {
      final completer = Completer<bool>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as bool;
    }
    return result;
  }
}

class ProcessPsbtCallback implements pj.ProcessPsbt {
  final FutureOr<String> Function(String) _callback;

  ProcessPsbtCallback(this._callback);

  @override
  String callback(String psbt) {
    final result = _callback(psbt);
    if (result is Future<String>) {
      final completer = Completer<String>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as String;
    }
    return result;
  }
}

class PayjoinNotFoundException extends BullException {
  PayjoinNotFoundException(super.message);
}

class ReceiveCreationException extends BullException {
  ReceiveCreationException(super.message);
}

class NoValidPayjoinBip21Exception extends BullException {
  NoValidPayjoinBip21Exception(super.message);
}

class PayjoinExpiredException extends BullException {
  PayjoinExpiredException(super.message);
}

class OhttpRelaysUnavailableException extends BullException {
  OhttpRelaysUnavailableException(super.message);
}
