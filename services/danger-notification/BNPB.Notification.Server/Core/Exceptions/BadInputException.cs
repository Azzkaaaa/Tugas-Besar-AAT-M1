namespace BNPB.Notification.Server.Core.Exceptions;

public class BadInputException : Exception
{
  public BadInputException() : base() { }
  public BadInputException(string message) : base(message) { }
  public BadInputException(string message, Exception innerException) : base(message, innerException) { }
}
