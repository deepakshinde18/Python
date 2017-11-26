function [J, grad] = costFunctionReg(theta, X, y, lambda)
%COSTFUNCTIONREG Compute cost and gradient for logistic regression with regularization
%   J = COSTFUNCTIONREG(theta, X, y, lambda) computes the cost of using
%   theta as the parameter for regularized logistic regression and the
%   gradient of the cost w.r.t. to the parameters. 

% Initialize some useful values
m = length(y); % number of training examples

% You need to return the following variables correctly 
J = 0;
grad = zeros(size(theta));

% ====================== YOUR CODE HERE ======================
% Instructions: Compute the cost of a particular choice of theta.
%               You should set J to the cost.
%               Compute the partial derivatives and set grad to the partial
%               derivatives of the cost w.r.t. each parameter in theta

h_theta = sigmoid(X*theta);
J = (1/m) * (-y' * log(h_theta) - (1-y)' * log(1-h_theta)) + (lambda/(2*m)) * (theta(2:length(theta)))' * theta(2:length(theta));

thetaZero = theta;
thetaZero(1) = 0;

grad = ((1 / m) * (h_theta - y)' * X) + lambda / m * thetaZero';



% tempTheta = theta;
% tempTheta(1) = 0;
% J = (-1 / m) * sum(y.*log(sigmoid(X * theta)) + (1 - y).*log(1 - sigmoid(X * theta))) + (lambda / (2 * m))*sum(tempTheta.^2);
% temp = sigmoid (X * theta);
% error = temp - y;
% grad = (1 / m) * (X' * error) + (lambda/m)*tempTheta;


% htheta = sigmoid(X * theta);
% J = 1 / m * sum(-y .* log(htheta) - (1 - y) .* log(1 - htheta)) + lambda / (2 * m) * sum(theta(2:end) .^ 2);
% grad(1) = 1 / m * sum((htheta - y) .* X(:, 1));
% for i = 2:size(theta, 1)
%     grad(i) = 1 / m * sum((htheta - y) .* X(:, i)) + lambda / m * theta(i);
% end




% =============================================================

end
